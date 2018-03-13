/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "tests/cql_test_env.hh"
#include "tests/result_set_assertions.hh"

#include "database.hh"
#include "partition_slice_builder.hh"
#include "frozen_mutation.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

SEASTAR_TEST_CASE(test_querying_with_limits) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            dht::partition_range_vector pranges;
            for (uint32_t i = 1; i <= 3; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(sprint("key%d", i)));
                mutation m(pkey, s);
                m.partition().apply(tombstone(api::timestamp_type(1), gc_clock::now()));
                db.apply(s, freeze(m)).get();
            }
            for (uint32_t i = 3; i <= 8; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(sprint("key%d", i)));
                mutation m(pkey, s);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", data_value(bytes("v1")), 1);
                db.apply(s, freeze(m)).get();
                pranges.emplace_back(dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*s, std::move(pkey))));
            }

            auto max_size = std::numeric_limits<size_t>::max();
            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), 3);
                auto result = db.query(s, cmd, query::result_request::only_result, pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::experimental::nullopt, 5);
                auto result = db.query(s, cmd, query::result_request::only_result, pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(5);
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(),
                        query::max_rows, gc_clock::now(), std::experimental::nullopt, 3);
                auto result = db.query(s, cmd, query::result_request::only_result, pranges, nullptr, max_size).get0();
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(3);
            }
        });
    });
}
