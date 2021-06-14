/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/seastar.hh>
#include "seastarx.hh"

namespace utils {

/**
 * 100% naive rate limiter. Consider it a placeholder
 * Will let you process X "units" per second, then reset this every s.
 * Obviously, accuracy is virtually non-existant and steady rate will fluctuate.
 */
class rate_limiter {
private:
    timer<lowres_clock> _timer;
    size_t _units_per_s;
    semaphore _sem {0};

    void on_timer();
public:
    rate_limiter(size_t rate);
    future<> reserve(size_t u);
};

}
