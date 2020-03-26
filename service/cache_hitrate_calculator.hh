/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma  once

#include "database_fwd.hh"
#include "utils/UUID.hh"
#include <seastar/core/timer.hh>
#include <seastar/core/sharded.hh>

using namespace seastar;

namespace service {

class cache_hitrate_calculator : public seastar::async_sharded_service<cache_hitrate_calculator>, public seastar::peering_sharded_service<cache_hitrate_calculator> {
    struct stat {
        float h = 0;
        float m = 0;
        stat& operator+=(stat& o) {
            h += o.h;
            m += o.m;
            return *this;
        }
    };

    seastar::sharded<database>& _db;
    timer<lowres_clock> _timer;
    bool _stopped = false;
    float _diff = 0;
    std::unordered_map<utils::UUID, stat> _rates;
    size_t _slen = 0;
    std::string _gstate;
    future<> _done = make_ready_future();

    future<lowres_clock::duration> recalculate_hitrates();
    void recalculate_timer();
public:
    cache_hitrate_calculator(seastar::sharded<database>& db);
    void run_on(size_t master, lowres_clock::duration d = std::chrono::milliseconds(2000));

    future<> stop();
};

}
