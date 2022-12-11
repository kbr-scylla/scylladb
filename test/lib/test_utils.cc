/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "test/lib/test_utils.hh"

#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <seastar/core/print.hh>
#include <seastar/util/backtrace.hh>
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "seastarx.hh"
#include <random>

namespace tests {

namespace {

std::string format_msg(std::string_view test_function_name, bool ok, std::source_location sl, std::string_view msg) {
    return fmt::format("{}(): {} @ {}() {}:{:d}{}{}", test_function_name, ok ? "OK" : "FAIL", sl.function_name(), sl.file_name(), sl.line(), msg.empty() ? "" : ": ", msg);
}

}

bool do_check(bool condition, std::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        testlog.error("{}", format_msg(__FUNCTION__, condition, sl, msg));
    }
    return condition;
}

void do_require(bool condition, std::source_location sl, std::string_view msg) {
    if (condition) {
        testlog.trace("{}", format_msg(__FUNCTION__, condition, sl, msg));
    } else {
        auto formatted_msg = format_msg(__FUNCTION__, condition, sl, msg);
        testlog.error("{}", formatted_msg);
        throw_with_backtrace<std::runtime_error>(std::move(formatted_msg));
    }

}

void fail(std::string_view msg, std::source_location sl) {
    throw_with_backtrace<std::runtime_error>(format_msg(__FUNCTION__, false, sl, msg));
}

}

sstring make_random_string(size_t size) {
    static thread_local std::default_random_engine rng;
    std::uniform_int_distribution<char> dist;
    sstring str = uninitialized_string(size);
    for (auto&& b : str) {
        b = dist(rng);
    }
    return str;
}

sstring make_random_numeric_string(size_t size) {
    static thread_local std::default_random_engine rng;
    std::uniform_int_distribution<char> dist('0', '9');
    sstring str = uninitialized_string(size);
    for (auto&& b : str) {
        b = dist(rng);
    }
    return str;
}

std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size, std::optional<shard_id> shard) {
    std::vector<std::pair<sstring, dht::decorated_key>> p;
    p.reserve(n);

    auto key_id = 0U;
    auto generated = 0U;
    while (generated < n) {
        auto raw_key = sstring(std::max(min_key_size, sizeof(key_id)), int8_t(0));
        std::copy_n(reinterpret_cast<int8_t*>(&key_id), sizeof(key_id), raw_key.begin());
        auto dk = dht::decorate_key(*s, partition_key::from_single_value(*s, to_bytes(raw_key)));
        key_id++;
        if (shard) {
            if (*shard != shard_of(*s, dk.token())) {
                continue;
            }
        }
        generated++;
        p.emplace_back(std::move(raw_key), std::move(dk));
    }
    boost::sort(p, [&] (auto& p1, auto& p2) {
        return p1.second.less_compare(*s, p2.second);
    });
    return boost::copy_range<std::vector<sstring>>(p | boost::adaptors::map_keys);
}

std::vector<sstring> do_make_keys(unsigned n, const schema_ptr& s, size_t min_key_size, local_shard_only lso) {
    return do_make_keys(n, s, min_key_size, lso ? std::optional(this_shard_id()) : std::nullopt);
}
