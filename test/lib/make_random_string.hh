/*
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include <random>

inline
sstring make_random_string(size_t size) {
    static thread_local std::default_random_engine rng;
    std::uniform_int_distribution<char> dist;
    sstring str = uninitialized_string(size);
    for (auto&& b : str) {
        b = dist(rng);
    }
    return str;
}
