/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cinttypes>

namespace query {

struct max_result_size {
    uint64_t soft_limit;
    uint64_t hard_limit;

    max_result_size() = delete;
    explicit max_result_size(uint64_t max_size) : soft_limit(max_size), hard_limit(max_size) { }
    explicit max_result_size(uint64_t soft_limit, uint64_t hard_limit) : soft_limit(soft_limit), hard_limit(hard_limit) { }
};

inline bool operator==(const max_result_size& a, const max_result_size& b) {
    return a.soft_limit == b.soft_limit && a.hard_limit == b.hard_limit;
}

}
