/*
 * Copyright (c) 2021, Arm Limited and affiliates. All rights reserved.
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

//
// Helper functions for raft tests
//

#pragma once

#define BOOST_TEST_MODULE raft

#include <boost/test/unit_test.hpp>
#include "test/lib/log.hh"
#include "serializer_impl.hh"
#include <limits>

#include "raft/fsm.hh"

using raft::term_t, raft::index_t, raft::server_id, raft::log_entry, raft::server_address_set;
using seastar::make_lw_shared;

void election_threshold(raft::fsm& fsm) {
    // Election threshold should be strictly less than
    // minimal randomized election timeout to make tests
    // stable, but enough to disable "stable leader" rule.
    for (int i = 0; i < raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void election_timeout(raft::fsm& fsm) {
    for (int i = 0; i <= 2 * raft::ELECTION_TIMEOUT.count(); i++) {
        fsm.tick();
    }
}

void make_candidate(raft::fsm& fsm) {
    assert(fsm.is_follower());
    // NOTE: single node skips candidate state
    while (fsm.is_follower()) {
        fsm.tick();
    }
}

struct trivial_failure_detector: public raft::failure_detector {
    bool is_alive(raft::server_id from) override {
        return true;
    }
} trivial_failure_detector;

class discrete_failure_detector: public raft::failure_detector {
    bool _is_alive = true;
    std::unordered_set<server_id> _dead;
public:
    bool is_alive(raft::server_id id) override {
        return _is_alive && !_dead.contains(id);
    }
    void mark_dead(server_id id) { _dead.emplace(id); }
    void mark_alive(server_id id) { _dead.erase(id); }
    void mark_all_dead() { _is_alive = false; }
};

template <typename T> void add_entry(raft::log& log, T cmd) {
    log.emplace_back(make_lw_shared<log_entry>(log_entry{log.last_term(), log.next_idx(), cmd}));
}

raft::snapshot log_snapshot(raft::log& log, index_t idx) {
    return raft::snapshot{.idx = idx, .term = log.last_term(), .config = log.get_snapshot().config};
}

template <typename T>
raft::command create_command(T val) {
    raft::command command;
    ser::serialize(command, val);

    return std::move(command);
}

raft::fsm_config fsm_cfg{.append_request_threshold = 1, .enable_prevoting = false};
raft::fsm_config fsm_cfg_pre{.append_request_threshold = 1, .enable_prevoting = true};

class fsm_debug : public raft::fsm {
public:
    using raft::fsm::fsm;
    void become_follower(server_id leader) {
        raft::fsm::become_follower(leader);
    }
    const raft::follower_progress& get_progress(server_id id) {
        raft::follower_progress* progress = leader_state().tracker.find(id);
        return *progress;
    }
    raft::log& get_log() {
        return raft::fsm::get_log();
    }
};

// NOTE: it doesn't compare data contents, just the data type
bool compare_log_entry(raft::log_entry_ptr le1, raft::log_entry_ptr le2) {
    if (le1->term != le2->term || le1->idx != le2->idx || le1->data.index() != le2->data.index()) {
        return false;
    }
    return true;
}

bool compare_log_entries(raft::log& log1, raft::log& log2, size_t from, size_t to) {
    assert(to <= log1.last_idx());
    assert(to <= log2.last_idx());
    for (size_t i = from; i <= to; ++i) {
        if (!compare_log_entry(log1[i], log2[i])) {
            return false;
        }
    }
    return true;
}

using raft_routing_map = std::unordered_map<raft::server_id, raft::fsm*>;

void
communicate_impl(std::function<bool()> stop_pred, raft_routing_map& map) {
    // To enable tracing, set:
    // global_logger_registry().set_all_loggers_level(seastar::log_level::trace);
    //
    bool has_traffic;
    do {
        has_traffic = false;
        for (auto e : map) {
            raft::fsm& from = *e.second;
            bool has_output;
            for (auto output = from.get_output(); !output.empty(); output = from.get_output()) {
                if (stop_pred()) {
                    return;
                }
                for (auto&& m : output.messages) {
                    has_traffic = true;
                    auto it = map.find(m.first);
                    if (it == map.end()) {
                        // The node is not available, drop the message
                        continue;
                    }
                    raft::fsm& to = *(it->second);
                    std::visit([&from, &to](auto&& m) { to.step(from.id(), std::move(m)); },
                        std::move(m.second));
                    if (stop_pred()) {
                        return;
                    }
                }
            }
        }
    } while (has_traffic);
}

template <typename... Args>
void communicate_until(std::function<bool()> stop_pred, Args&&... args) {
    raft_routing_map map;
    auto add_map_entry = [&map](raft::fsm& fsm) -> void {
        map.emplace(fsm.id(), &fsm);
    };
    (add_map_entry(args), ...);
    communicate_impl(stop_pred, map);
}

template <typename... Args>
void communicate(Args&&... args) {
    return communicate_until([]() { return false; }, std::forward<Args>(args)...);
}

template <typename... Args>
raft::fsm* select_leader(Args&&... args) {
    raft::fsm* leader = nullptr;
    auto assign_leader = [&leader](raft::fsm& fsm) {
        if (fsm.is_leader()) {
            leader = &fsm;
            return false;
        }
        return true;
    };
    (assign_leader(args) && ...);
    BOOST_CHECK(leader);
    return leader;
}


raft::server_id id() {
    static int id = 0;
    return raft::server_id{utils::UUID(0, ++id)};
}

raft::server_address_set address_set(std::initializer_list<raft::server_id> ids) {
    raft::server_address_set set;
    for (auto id : ids) {
        set.emplace(raft::server_address{.id = id});
    }
    return set;
}

raft::fsm create_follower(raft::server_id id, raft::log log, raft::failure_detector& fd = trivial_failure_detector) {
    return raft::fsm(id, term_t{}, server_id{}, std::move(log), fd, fsm_cfg);
}

