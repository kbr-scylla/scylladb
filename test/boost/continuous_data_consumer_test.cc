/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "vint-serialization.hh"
#include "sstables/consumer.hh"

#include "bytes.hh"
#include "utils/buffer_input_stream.hh"
#include "test/lib/reader_permit.hh"
#include "test/lib/random_utils.hh"

#include <boost/test/unit_test.hpp>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <random>

namespace {

class test_consumer final : public data_consumer::continuous_data_consumer<test_consumer> {
    static const int MULTIPLIER = 10;
    uint64_t _tested_value;
    int _state = 0;
    int _count = 0;

    void check(uint64_t got) {
        BOOST_REQUIRE_EQUAL(_tested_value, got);
    }

    static uint64_t calculate_length(uint64_t tested_value) {
        return MULTIPLIER * unsigned_vint::serialized_size(tested_value);
    }

    static input_stream<char> prepare_stream(uint64_t tested_value) {
        temporary_buffer<char> buf(calculate_length(tested_value));
        int pos = 0;
        bytes::value_type* out = reinterpret_cast<bytes::value_type*>(buf.get_write());
        for (int i = 0; i < MULTIPLIER; ++i) {
            pos += unsigned_vint::serialize(tested_value, out + pos);
        }
        return make_buffer_input_stream(std::move(buf), [] {return 1;});
    }

public:
    test_consumer(uint64_t tested_value)
        : continuous_data_consumer(tests::make_permit(), prepare_stream(tested_value), 0, calculate_length(tested_value))
        , _tested_value(tested_value)
    { }

    bool non_consuming() { return false; }

    void verify_end_state() {}

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        switch (_state) {
        case 0:
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = 1;
                break;
            }
            // fall-through
        case 1:
            check(_u64);
            ++_count;
            _state = _count < MULTIPLIER ? 0 : 2;
            break;
        default:
            BOOST_FAIL("wrong consumer state");
        }
        return _state == 2 ? data_consumer::proceed::no : data_consumer::proceed::yes;
    }

    void run() {
        consume_input().get();
    }
};

}

SEASTAR_THREAD_TEST_CASE(test_read_unsigned_vint) {
    auto nr_tests =
#ifdef SEASTAR_DEBUG
            10
#else
            1000
#endif
            ;
    test_consumer(0).run();
    for (int highest_bit = 0; highest_bit < 64; ++highest_bit) {
        uint64_t tested_value = uint64_t{1} << highest_bit;
        for (int i = 0; i < nr_tests; ++i) {
            test_consumer(tested_value + tests::random::get_int<uint64_t>(tested_value - 1)).run();
        }
    }
}

