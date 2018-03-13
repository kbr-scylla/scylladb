/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma  once

#include "database.hh"
#include "core/timer.hh"
#include "core/sharded.hh"

namespace service {

class cache_hitrate_calculator : public seastar::async_sharded_service<cache_hitrate_calculator> {
    seastar::sharded<database>& _db;
    seastar::sharded<cache_hitrate_calculator>& _me;
    timer<lowres_clock> _timer;
    bool _stopped = false;
    float _diff = 0;

    future<lowres_clock::duration> recalculate_hitrates();
    void recalculate_timer();
public:
    cache_hitrate_calculator(seastar::sharded<database>& db, seastar::sharded<cache_hitrate_calculator>& me);
    void run_on(size_t master, lowres_clock::duration d = std::chrono::milliseconds(2000));

    future<> stop();
};

}
