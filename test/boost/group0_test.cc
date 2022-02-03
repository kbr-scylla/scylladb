/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "utils/UUID_gen.hh"
#include "transport/messages/result_message.hh"
#include "service/migration_manager.hh"

static future<utils::chunked_vector<std::vector<bytes_opt>>> fetch_rows(cql_test_env& e, std::string_view cql) {
    auto msg = co_await e.execute_cql(cql);
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    BOOST_REQUIRE(rows);
    co_return rows->rs().result_set().rows();
}

static future<size_t> get_history_size(cql_test_env& e) {
    co_return (co_await fetch_rows(e, "select * from system.group0_history")).size();
}

SEASTAR_TEST_CASE(test_group0_history_clearing_old_entries) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        using namespace std::chrono;

        auto get_history_size = std::bind_front(::get_history_size, std::ref(e));

        auto perform_schema_change = [&, has_ks = false] () mutable -> future<> {
            if (has_ks) {
                co_await e.execute_cql("drop keyspace new_ks");
            } else {
                co_await e.execute_cql("create keyspace new_ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            }
            has_ks = !has_ks;
        };

        auto size = co_await get_history_size();
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), size + 1);

        auto& mm = e.migration_manager().local();
        mm.set_group0_history_gc_duration(gc_clock::duration{0});

        // When group0_history_gc_duration is 0, any change should clear all previous history entries.
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);

        mm.set_group0_history_gc_duration(duration_cast<gc_clock::duration>(weeks{1}));
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 2);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        // Would use a shorter sleep, but gc_clock's resolution is one second.
        auto sleep_dur = seconds{1};
        co_await sleep(sleep_dur);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        auto get_history_timestamps = [&] () -> future<std::vector<milliseconds>> {
            auto rows = co_await fetch_rows(e, "select state_id from system.group0_history");
            std::vector<milliseconds> result;
            for (auto& row: rows) {
                auto state_id = value_cast<utils::UUID>(timeuuid_type->deserialize(*row[0]));
                result.push_back(utils::UUID_gen::unix_timestamp(state_id));
            }
            co_return result;
        };

        auto timestamps1 = co_await get_history_timestamps();
        mm.set_group0_history_gc_duration(duration_cast<gc_clock::duration>(sleep_dur));
        co_await perform_schema_change();
        auto timestamps2 = co_await get_history_timestamps();
        // State IDs are sorted in descending order in the history table.
        // The first entry corresponds to the last schema change.
        auto last_ts = timestamps2.front();

        // All entries in `timestamps2` except `last_ts` should be present in `timestamps1`.
        BOOST_REQUIRE(std::includes(timestamps1.begin(), timestamps1.end(), timestamps2.begin()+1, timestamps2.end(), std::greater{}));

        // Count the number of timestamps in `timestamps1` that are older than the last entry by `sleep_dur` or more.
        // There should be about 12 because we slept for `sleep_dur` between the two loops above
        // and performing these schema changes should be much faster than `sleep_dur`.
        auto older_by_sleep_dur = std::count_if(timestamps1.begin(), timestamps1.end(), [last_ts, sleep_dur] (milliseconds ts) {
            return last_ts - ts > sleep_dur;
        });

        testlog.info("older by sleep_dur: {}", older_by_sleep_dur);

        // That last change should have cleared exactly those older than `sleep_dur` entries.
        // Therefore `timestamps2` should contain all in `timestamps1` minus those changes plus one (`last_ts`).
        BOOST_REQUIRE_EQUAL(timestamps2.size(), timestamps1.size() - older_by_sleep_dur + 1);

    }, raft_cql_test_config());
}

SEASTAR_TEST_CASE(test_concurrent_group0_modifications) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        auto& mm = e.migration_manager().local();

        // migration_manager::_group0_operation_mutex prevents concurrent group 0 changes to be executed on a single node,
        // so in production `group0_concurrent_modification` never occurs if all changes go through a single node.
        // For this test, give it more units so it doesn't block these concurrent executions
        // in order to simulate a scenario where multiple nodes concurrently send schema changes.
        mm.group0_operation_mutex().signal(1337);

        // Make DDL statement execution fail on the first attempt if it gets a concurrent modification exception.
        mm.set_concurrent_ddl_retries(0);

        auto get_history_size = std::bind_front(::get_history_size, std::ref(e));

        auto perform_schema_changes = [] (cql_test_env& e, size_t n, size_t task_id) -> future<size_t> {
            size_t successes = 0;
            bool has_ks = false;
            auto drop_ks_cql = format("drop keyspace new_ks{}", task_id);
            auto create_ks_cql = format("create keyspace new_ks{} with replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}", task_id);

            auto perform = [&] () -> future<> {
                try {
                    if (has_ks) {
                        co_await e.execute_cql(drop_ks_cql);
                    } else {
                        co_await e.execute_cql(create_ks_cql);
                    }
                    has_ks = !has_ks;
                    ++successes;
                } catch (const service::group0_concurrent_modification&) {}
            };

            while (n--) {
                co_await perform();
            }

            co_return successes;
        };

        auto size = co_await get_history_size();

        size_t N = 4;
        size_t M = 4;

        // Run N concurrent tasks, each performing M schema changes in sequence.
        auto successes = co_await map_reduce(boost::irange(size_t{0}, N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // The number of new entries that appeared in group 0 history table should be exactly equal
        // to the number of successful schema changes.
        BOOST_REQUIRE_EQUAL(successes, (co_await get_history_size()) - size);

        // Make it so that execution of a DDL statement will perform up to (N-1) * M + 1 attempts (first try + up to (N-1) * M retries).
        mm.set_concurrent_ddl_retries((N-1)*M);

        // Run N concurrent tasks, each performing M schema changes in sequence.
        // (use different range of task_ids so the new tasks' statements don't conflict with existing keyspaces from previous tasks)
        successes = co_await map_reduce(boost::irange(N, 2*N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // Each task performs M schema changes. There are N tasks.
        // Thus, for each task, all other tasks combined perform (N-1) * M schema changes.
        // Each `group0_concurrent_modification` exception means that some statement executed successfully in another task.
        // Thus, each statement can get at most (N-1) * M `group0_concurrent_modification` exceptions.
        // Since we configured the system to perform (N-1) * M + 1 attempts, the last attempt should always succeed even if all previous
        // ones failed - because that means every other task has finished its work.
        // Thus, `group0_concurrent_modification` should never propagate outside `execute_cql`.
        // Therefore the number of successes should be the number of calls to `execute_cql`, which is N*M in total.
        BOOST_REQUIRE_EQUAL(successes, N*M);

        // Let's verify that the mutex indeed does its job.
        mm.group0_operation_mutex().consume(1337);
        mm.set_concurrent_ddl_retries(0);

        successes = co_await map_reduce(boost::irange(2*N, 3*N), std::bind_front(perform_schema_changes, std::ref(e), M), 0, std::plus{});

        // Each execution should have succeeded on first attempt because the mutex serialized them all.
        BOOST_REQUIRE_EQUAL(successes, N*M);

    }, raft_cql_test_config());
}
