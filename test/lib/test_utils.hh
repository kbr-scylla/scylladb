/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <experimental/source_location>
#include <string>

#include <fmt/format.h>

// Thread safe alternatives to BOOST_REQUIRE_*, BOOST_CHECK_* and BOOST_FAIL().
// Use these if instead of the BOOST provided macros if you want to use them on
// multiple shards, to avoid problems due to the BOOST versions not being thread
// safe.

namespace tests {

void do_check(bool condition, std::experimental::source_location sl, std::string_view msg);

inline void check(bool condition, std::experimental::source_location sl = std::experimental::source_location::current()) {
    do_check(condition, sl, {});
}

template <typename LHS, typename RHS>
void check_equal(const LHS& lhs, const RHS& rhs, std::experimental::source_location sl = std::experimental::source_location::current()) {
    const auto condition = (lhs == rhs);
    do_check(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void do_require(bool condition, std::experimental::source_location sl, std::string_view msg);

inline void require(bool condition, std::experimental::source_location sl = std::experimental::source_location::current()) {
    do_require(condition, sl, {});
}

template <typename LHS, typename RHS>
void require_equal(const LHS& lhs, const RHS& rhs, std::experimental::source_location sl = std::experimental::source_location::current()) {
    const auto condition = (lhs == rhs);
    do_require(condition, sl, fmt::format("{} {}= {}", lhs, condition ? "=" : "!", rhs));
}

void fail(std::string_view msg, std::experimental::source_location sl = std::experimental::source_location::current());

}
