/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <chrono>
/**
 * A helper class to keep track of latencies
 */
namespace utils {

class latency_counter {
public:
    using clock = std::chrono::steady_clock;
    using time_point = clock::time_point;
    using duration = clock::duration;
private:
    time_point _start;
    time_point _stop;
public:
    void start() {
        _start = now();
    }

    bool is_start() const {
        // if start is not set it is still zero
        return _start.time_since_epoch().count();
    }
    latency_counter& stop() {
        _stop = now();
        return *this;
    }

    bool is_stopped() const {
        // if stop was not set, it is still zero
        return _stop.time_since_epoch().count();
    }

    duration latency() const {
        return _stop - _start;
    }

    latency_counter& check_and_stop() {
        if (!is_stopped()) {
            return stop();
        }
        return *this;
    }

    static time_point now() {
        return clock::now();
    }
};

}
