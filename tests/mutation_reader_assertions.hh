/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <boost/test/unit_test.hpp>
#include "mutation_reader.hh"
#include "mutation_assertions.hh"

// Intended to be called in a seastar thread
class reader_assertions {
    mutation_reader _reader;
    dht::partition_range _pr;
private:
    mutation_opt read_next() {
        auto smo = _reader().get0();
        return mutation_from_streamed_mutation(std::move(smo)).get0();
    }
public:
    reader_assertions(mutation_reader reader)
        : _reader(std::move(reader))
    { }

    reader_assertions& produces(const dht::decorated_key& dk) {
        BOOST_TEST_MESSAGE(sprint("Expecting key %s", dk));
        auto mo = read_next();
        if (!mo) {
            BOOST_FAIL(sprint("Expected: %s, got end of stream", dk));
        }
        if (!mo->decorated_key().equal(*mo->schema(), dk)) {
            BOOST_FAIL(sprint("Expected: %s, got: %s", dk, mo->decorated_key()));
        }
        return *this;
    }

    reader_assertions& produces(const mutation& m, const stdx::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        BOOST_TEST_MESSAGE(sprint("Expecting %s", m));
        auto mo = read_next();
        BOOST_REQUIRE(bool(mo));
        memory::disable_failure_guard dfg;
        assert_that(*mo).is_equal_to(m, ck_ranges);
        return *this;
    }

    reader_assertions& produces_compacted(const mutation& m, const stdx::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        BOOST_TEST_MESSAGE(sprint("Expecting after compaction: %s", m));
        auto mo = read_next();
        BOOST_REQUIRE(bool(mo));
        memory::disable_failure_guard dfg;
        mutation got = *mo;
        got.partition().compact_for_compaction(*m.schema(), always_gc, gc_clock::now());
        assert_that(got).is_equal_to(m, ck_ranges);
        return *this;
    }

    mutation_assertion next_mutation() {
        auto mo = read_next();
        BOOST_REQUIRE(bool(mo));
        return mutation_assertion(std::move(*mo));
    }

    template<typename RangeOfMutations>
    reader_assertions& produces(const RangeOfMutations& mutations) {
        for (auto&& m : mutations) {
            produces(m);
        }
        return *this;
    }

    reader_assertions& produces_end_of_stream() {
        BOOST_TEST_MESSAGE("Expecting end of stream");
        auto mo = read_next();
        if (bool(mo)) {
            BOOST_FAIL(sprint("Expected end of stream, got %s", *mo));
        }
        return *this;
    }

    reader_assertions& produces_eos_or_empty_mutation() {
        BOOST_TEST_MESSAGE("Expecting eos or empty mutation");
        auto mo = read_next();
        if (mo) {
            if (!mo->partition().empty()) {
                BOOST_FAIL(sprint("Mutation is not empty: %s", *mo));
            }
        }
        return *this;
    }

    reader_assertions& fast_forward_to(const dht::partition_range& pr) {
        _pr = pr;
        _reader.fast_forward_to(_pr).get0();
        return *this;
    }
};

inline
reader_assertions assert_that(mutation_reader r) {
    return { std::move(r) };
}
