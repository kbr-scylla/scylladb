/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <cstdint>
#include <variant>
#include <seastar/util/bool_class.hh>

namespace db {

using allow_per_partition_rate_limit = seastar::bool_class<class allow_per_partition_rate_limit_tag>;

namespace per_partition_rate_limit {

// Tells the replica to account the operation (increase the corresponding counter)
// and accept it regardless from the value of the counter.
//
// Used when the coordinator IS a replica (correct node and shard).
struct account_only {};

// Tells the replica to account the operation and decide whether to reject
// or not, based on the random variable sent by the coordinator.
//
// Used when the coordinator IS NOT a replica (wrong node or shard).
struct account_and_enforce {
    // A random 32-bit number generated by the coordinator.
    // Replicas are supposed to use it in order to decide whether
    // to accept or reject.
    uint32_t random_variable;

    inline double get_random_variable_as_double() const {
        return double(random_variable) / double(1LL << 32);
    }
};

// std::monostate -> do not count to the rate limit and never reject
// account_and_enforce -> account to the rate limit and optionally reject
using info = std::variant<std::monostate, account_only, account_and_enforce>;

} // namespace per_partition_rate_limit

} // namespace db

