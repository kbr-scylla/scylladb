/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

// A coarser and faster version of std::steady_clock, using
// CLOCK_MONOTONIC_COARSE instead of CLOCK_MONOTONIC.
//
// Intended for measuring time taken by synchronous code paths (where
// seastar::lowres_clock is not suitable).

#include <chrono>
#include <ctime>

namespace utils {

struct coarse_steady_clock {
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    using time_point = std::chrono::time_point<coarse_steady_clock, duration>;

    static constexpr bool is_steady = true;

    static time_point now() noexcept {
        timespec tp;
        clock_gettime(CLOCK_MONOTONIC_COARSE, &tp);
        return time_point(std::chrono::seconds(tp.tv_sec) + std::chrono::nanoseconds(tp.tv_nsec));
    };
};

};
