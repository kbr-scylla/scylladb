/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

//
// Helper functions for raft tests
//

#include "helpers.hh"

raft::fsm_config fsm_cfg{.append_request_threshold = 1, .enable_prevoting = false};
raft::fsm_config fsm_cfg_pre{.append_request_threshold = 1, .enable_prevoting = true};

struct trivial_failure_detector trivial_failure_detector;

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

raft::snapshot_descriptor log_snapshot(raft::log& log, raft::index_t idx) {
    return raft::snapshot_descriptor{.idx = idx, .term = log.last_term(), .config = log.get_snapshot().config};
}

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

bool deliver(raft_routing_map& routes, raft::server_id from,
        std::pair<raft::server_id, raft::rpc_message> m) {
    auto it = routes.find(m.first);
    if (it == routes.end()) {
        // Destination not available
        return false;
    }
    std::visit([from, &to = *it->second] (auto&& m) { to.step(from, std::move(m)); }, std::move(m.second));
    return true;
}

void deliver(raft_routing_map& routes, raft::server_id from, std::vector<std::pair<raft::server_id, raft::rpc_message>> msgs) {
    for (auto& m: msgs) {
        deliver(routes, from, std::move(m));
    }
}

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
                    if (deliver(map, from.id(), std::move(m)) && stop_pred()) {
                        return;
                    }
                }
            }
        }
    } while (has_traffic);
}

raft::server_id id() {
    static int id = 0;
    return raft::server_id{utils::UUID(0, ++id)};
}

raft::server_address_set address_set(std::vector<raft::server_id> ids) {
    raft::server_address_set set;
    for (auto id : ids) {
        set.emplace(raft::server_address{.id = id});
    }
    return set;
}

fsm_debug create_follower(raft::server_id id, raft::log log, raft::failure_detector& fd) {
    return fsm_debug(id, raft::term_t{}, raft::server_id{}, std::move(log), fd, fsm_cfg);
}


// Raft uses UUID 0 as special case.
// Convert local 0-based integer id to raft +1 UUID
utils::UUID to_raft_uuid(size_t int_id) {
    return utils::UUID{0, int_id + 1};
}

raft::server_id to_raft_id(size_t int_id) {
    return raft::server_id{to_raft_uuid(int_id)};
}

// NOTE: can_vote = true
raft::server_address to_server_address(size_t int_id) {
    return raft::server_address{raft::server_id{to_raft_uuid(int_id)}};
}

size_t to_int_id(utils::UUID uuid) {
    return uuid.get_least_significant_bits() - 1;
}

// Return true upon a random event with given probability
bool rolladice(float probability) {
    return tests::random::get_real(0.0, 1.0) < probability;
}
