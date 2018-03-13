/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include "utils/crc.hh"
#include <seastar/core/print.hh>

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

inline
uint32_t
do_compute_crc(utils::crc32& c) {
    return c.get();
}

template <typename T, typename... Rest>
inline
uint32_t
do_compute_crc(utils::crc32& c, const T& val, const Rest&... rest) {
    c.process(val);
    return do_compute_crc(c, rest...);
}

template <typename... T>
inline
uint32_t
compute_crc(const T&... vals) {
    utils::crc32 c;
    return do_compute_crc(c, vals...);
}

BOOST_AUTO_TEST_CASE(crc_1_vs_4) {
    using b = uint8_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), b(3), b(2), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_121_vs_4) {
    using b = uint8_t;
    using w = uint16_t;
    BOOST_REQUIRE_EQUAL(compute_crc(0x01020304), compute_crc(b(4), w(0x0203), b(1)));
}

BOOST_AUTO_TEST_CASE(crc_44_vs_8) {
    using q = uint64_t;
    BOOST_REQUIRE_EQUAL(compute_crc(q(0x0102030405060708)), compute_crc(0x05060708, 0x01020304));
}
