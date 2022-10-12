/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */


#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/coroutine.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/result_set_assertions.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"

#include "replica/database.hh"
#include "utils/lister.hh"
#include "partition_slice_builder.hh"
#include "frozen_mutation.hh"
#include "test/lib/mutation_source_test.hh"
#include "schema_registry.hh"
#include "service/migration_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "db/commitlog/commitlog.hh"
#include "test/lib/tmpdir.hh"
#include "db/data_listeners.hh"
#include "multishard_mutation_query.hh"
#include "transport/messages/result_message.hh"
#include "db/snapshot-ctl.hh"

using namespace std::chrono_literals;

class database_test {
    replica::database& _db;
public:
    explicit database_test(replica::database& db) : _db(db) { }

    reader_concurrency_semaphore& get_user_read_concurrency_semaphore() {
        return _db.read_concurrency_sem();
    }
    reader_concurrency_semaphore& get_streaming_read_concurrency_semaphore() {
        return _db._streaming_concurrency_sem;
    }
    reader_concurrency_semaphore& get_system_read_concurrency_semaphore() {
        return _db._system_read_concurrency_sem;
    }

    size_t get_total_user_reader_concurrency_semaphore_memory() {
        return _db._reader_concurrency_semaphores_group._total_memory;
    }

    size_t get_total_user_reader_concurrency_semaphore_weight() {
        return _db._reader_concurrency_semaphores_group._total_weight;
    }
};

static future<> apply_mutation(sharded<replica::database>& sharded_db, table_id uuid, const mutation& m, bool do_flush = false,
        db::commitlog::force_sync fs = db::commitlog::force_sync::no, db::timeout_clock::time_point timeout = db::no_timeout) {
    auto shard = m.shard_of();
    return sharded_db.invoke_on(shard, [uuid, fm = freeze(m), do_flush, fs, timeout] (replica::database& db) {
        auto& t = db.find_column_family(uuid);
        return db.apply(t.schema(), fm, tracing::trace_state_ptr(), fs, timeout).then([do_flush, &t] {
            return do_flush ? t.flush() : make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(test_safety_after_truncate) {
    auto cfg = make_shared<db::config>();
    cfg->auto_snapshot.set(false);
    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        auto& db = e.local_db();
        sstring ks_name = "ks";
        sstring cf_name = "cf";
        auto s = db.find_schema(ks_name, cf_name);
        auto uuid = s->id();

        std::vector<size_t> keys_per_shard;
        std::vector<dht::partition_range_vector> pranges_per_shard;
        keys_per_shard.resize(smp::count);
        pranges_per_shard.resize(smp::count);
        for (uint32_t i = 1; i <= 1000; ++i) {
            auto pkey = partition_key::from_single_value(*s, to_bytes(fmt::format("key{}", i)));
            mutation m(s, pkey);
            m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), {});
            auto shard = m.shard_of();
            keys_per_shard[shard]++;
            pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            apply_mutation(e.db(), uuid, m).get();
        }

        auto assert_query_result = [&] (const std::vector<size_t>& expected_sizes) {
            auto max_size = std::numeric_limits<size_t>::max();
            auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                    query::tombstone_limit::max, query::row_limit(1000));
            e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                auto shard = this_shard_id();
                auto s = db.find_schema(uuid);
                auto&& [result, cache_tempature] = co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout);
                assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_sizes[shard]);
            }).get();
        };
        assert_query_result(keys_per_shard);

        replica::database::truncate_table_on_all_shards(e.db(), "ks", "cf").get();

        for (auto it = keys_per_shard.begin(); it < keys_per_shard.end(); ++it) {
            *it = 0;
        }
        assert_query_result(keys_per_shard);

        e.db().invoke_on_all([&] (replica::database& db) -> future<> {
            auto cl = db.commitlog();
            auto rp = co_await db::commitlog_replayer::create_replayer(e.db());
            auto paths = co_await cl->list_existing_segments();
            co_await rp.recover(paths, db::commitlog::descriptor::FILENAME_PREFIX);
        }).get();

        assert_query_result(keys_per_shard);
        return make_ready_future<>();
    }, cfg);
}

// Reproducer for:
//   https://github.com/scylladb/scylla/issues/10421
//   https://github.com/scylladb/scylla/issues/10423
SEASTAR_TEST_CASE(test_truncate_without_snapshot_during_writes) {
    auto cfg = make_shared<db::config>();
    cfg->auto_snapshot.set(false);
    return do_with_cql_env_thread([] (cql_test_env& e) {
        sstring ks_name = "ks";
        sstring cf_name = "cf";
        e.execute_cql(fmt::format("create table {}.{} (k text, v int, primary key (k));", ks_name, cf_name)).get();
        auto& db = e.local_db();
        auto uuid = db.find_uuid(ks_name, cf_name);
        auto s = db.find_column_family(uuid).schema();
        int count = 0;

        auto insert_data = [&] (uint32_t begin, uint32_t end) {
            return parallel_for_each(boost::irange(begin, end), [&] (auto i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(fmt::format("key-{}", tests::random::get_int<uint64_t>())));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), {});
                auto shard = m.shard_of();
                return apply_mutation(e.db(), uuid, m, true /* do_flush */).finally([&] {
                    ++count;
                });
            });
        };

        uint32_t num_keys = 1000;

        auto f0 = insert_data(0, num_keys);
        auto f1 = do_until([&] { return count >= num_keys; }, [&, ts = db_clock::now()] {
            return replica::database::truncate_table_on_all_shards(e.db(), "ks", "cf", ts, false /* with_snapshot */).then([] {
                return yield();
            });
        });
        f0.get();
        f1.get();
    }, cfg);
}

SEASTAR_TEST_CASE(test_querying_with_limits) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
            auto& db = e.local_db();
            auto s = db.find_schema("ks", "cf");
            auto uuid = s->id();
            std::vector<size_t> keys_per_shard;
            std::vector<dht::partition_range_vector> pranges_per_shard;
            keys_per_shard.resize(smp::count);
            pranges_per_shard.resize(smp::count);
            for (uint32_t i = 1; i <= 3 * smp::count; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.partition().apply(tombstone(api::timestamp_type(1), gc_clock::now()));
                apply_mutation(e.db(), uuid, m).get();
                auto shard = m.shard_of();
                pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            }
            for (uint32_t i = 3 * smp::count; i <= 8 * smp::count; ++i) {
                auto pkey = partition_key::from_single_value(*s, to_bytes(format("key{:d}", i)));
                mutation m(s, pkey);
                m.set_clustered_cell(clustering_key_prefix::make_empty(), "v", int32_t(42), 1);
                apply_mutation(e.db(), uuid, m).get();
                auto shard = m.shard_of();
                keys_per_shard[shard]++;
                pranges_per_shard[shard].emplace_back(dht::partition_range::make_singular(dht::decorate_key(*s, std::move(pkey))));
            }

            auto max_size = std::numeric_limits<size_t>::max();
            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(3));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 3);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(query::max_rows), query::partition_limit(5));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 5);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }

            {
                auto cmd = query::read_command(s->id(), s->version(), partition_slice_builder(*s).build(), query::max_result_size(max_size),
                        query::tombstone_limit::max, query::row_limit(query::max_rows), query::partition_limit(3));
                e.db().invoke_on_all([&] (replica::database& db) -> future<> {
                    auto shard = this_shard_id();
                    auto s = db.find_schema(uuid);
                    auto result = std::get<0>(co_await db.query(s, cmd, query::result_options::only_result(), pranges_per_shard[shard], nullptr, db::no_timeout));
                    auto expected_size = std::min<size_t>(keys_per_shard[shard], 3);
                    assert_that(query::result_set::from_raw_result(s, cmd.slice, *result)).has_size(expected_size);
                }).get();
            }
        });
    });
}

static void test_database(void (*run_tests)(populate_fn_ex, bool)) {
    do_with_cql_env_thread([run_tests] (cql_test_env& e) {
        run_tests([&] (schema_ptr s, const std::vector<mutation>& partitions, gc_clock::time_point) -> mutation_source {
            auto& mm = e.migration_manager().local();
            try {
                auto group0_guard = mm.start_group0_operation().get();
                auto ts = group0_guard.write_timestamp();
                e.local_db().find_column_family(s->ks_name(), s->cf_name());
                mm.announce(mm.prepare_column_family_drop_announcement(s->ks_name(), s->cf_name(), ts).get(), std::move(group0_guard)).get();
            } catch (const replica::no_such_column_family&) {
                // expected
            }
            auto group0_guard = mm.start_group0_operation().get();
            auto ts = group0_guard.write_timestamp();
            mm.announce(mm.prepare_new_column_family_announcement(s, ts).get(), std::move(group0_guard)).get();
            replica::column_family& cf = e.local_db().find_column_family(s);
            auto uuid = cf.schema()->id();
            for (auto&& m : partitions) {
                apply_mutation(e.db(), uuid, m).get();
            }
            cf.flush().get();
            cf.get_row_cache().invalidate(row_cache::external_updater([] {})).get();
            return mutation_source([&] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr) {
                return cf.make_reader_v2(s, std::move(permit), range, slice, pc, std::move(trace_state), fwd, fwd_mr);
            });
        }, true);
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_plain) {
    test_database(run_mutation_source_tests_plain);
}

SEASTAR_THREAD_TEST_CASE(test_database_with_data_in_sstables_is_a_mutation_source_reverse) {
    test_database(run_mutation_source_tests_reverse);
}

SEASTAR_THREAD_TEST_CASE(test_distributed_loader_with_incomplete_sstables) {
    using sst = sstables::sstable;

    tmpdir data_dir;
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.data_file_directories({data_dir.path().string()}, db::config::config_source::CommandLine);

    // Create incomplete sstables in test data directory
    sstring ks = "system";
    sstring cf = "peers-37f71aca7dc2383ba70672528af04d4f";
    sstring sst_dir = (data_dir.path() / std::string_view(ks) / std::string_view(cf)).string();

    auto require_exist = [] (const sstring& name, bool should_exist) {
        auto exists = file_exists(name).get0();
        BOOST_REQUIRE(exists == should_exist);
    };

    auto touch_dir = [&require_exist] (const sstring& dir_name) {
        recursive_touch_directory(dir_name).get();
        require_exist(dir_name, true);
    };

    auto touch_file = [&require_exist] (const sstring& file_name) {
        auto f = open_file_dma(file_name, open_flags::create).get0();
        f.close().get();
        require_exist(file_name, true);
    };

    auto temp_sst_dir = sst::temp_sst_dir(sst_dir, generation_from_value(2));
    touch_dir(temp_sst_dir);

    temp_sst_dir = sst::temp_sst_dir(sst_dir, generation_from_value(3));
    touch_dir(temp_sst_dir);
    auto temp_file_name = sst::filename(temp_sst_dir, ks, cf, sst::version_types::mc, generation_from_value(3), sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name);

    temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, generation_from_value(4), sst::format_types::big, component_type::TemporaryTOC);
    touch_file(temp_file_name);
    temp_file_name = sst::filename(sst_dir, ks, cf, sst::version_types::mc, generation_from_value(4), sst::format_types::big, component_type::Data);
    touch_file(temp_file_name);

    do_with_cql_env_thread([&sst_dir, &ks, &cf, &require_exist] (cql_test_env& e) {
        require_exist(sst::temp_sst_dir(sst_dir, generation_from_value(2)), false);
        require_exist(sst::temp_sst_dir(sst_dir, generation_from_value(3)), false);

        require_exist(sst::filename(sst_dir, ks, cf, sst::version_types::mc, generation_from_value(4), sst::format_types::big, component_type::TemporaryTOC), false);
        require_exist(sst::filename(sst_dir, ks, cf, sst::version_types::mc, generation_from_value(4), sst::format_types::big, component_type::Data), false);
    }, db_cfg_ptr).get();
}

SEASTAR_THREAD_TEST_CASE(test_distributed_loader_with_pending_delete) {
    using sst = sstables::sstable;

    tmpdir data_dir;
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.data_file_directories({data_dir.path().string()}, db::config::config_source::CommandLine);

    // Create incomplete sstables in test data directory
    sstring ks = "system";
    sstring cf = "peers-37f71aca7dc2383ba70672528af04d4f";
    sstring sst_dir = (data_dir.path() / std::string_view(ks) / std::string_view(cf)).string();
    sstring pending_delete_dir = sst_dir + "/" + sst::pending_delete_dir_basename();

    auto require_exist = [] (const sstring& name, bool should_exist) {
        auto exists = file_exists(name).get0();
        if (should_exist) {
            BOOST_REQUIRE(exists);
        } else {
            BOOST_REQUIRE(!exists);
        }
    };

    auto touch_dir = [&require_exist] (const sstring& dir_name) {
        recursive_touch_directory(dir_name).get();
        require_exist(dir_name, true);
    };

    auto touch_file = [&require_exist] (const sstring& file_name) {
        auto f = open_file_dma(file_name, open_flags::create).get0();
        f.close().get();
        require_exist(file_name, true);
    };

    auto write_file = [&require_exist] (const sstring& file_name, const sstring& text) {
        auto f = open_file_dma(file_name, open_flags::wo | open_flags::create | open_flags::truncate).get0();
        auto os = make_file_output_stream(f, file_output_stream_options{}).get0();
        os.write(text).get();
        os.flush().get();
        os.close().get();
        require_exist(file_name, true);
    };

    auto component_basename = [&ks, &cf] (int64_t gen, component_type ctype) {
        return sst::component_basename(ks, cf, sst::version_types::mc, generation_from_value(gen), sst::format_types::big, ctype);
    };

    auto gen_filename = [&sst_dir, &ks, &cf] (int64_t gen, component_type ctype) {
        return sst::filename(sst_dir, ks, cf, sst::version_types::mc, generation_from_value(gen), sst::format_types::big, ctype);
    };

    touch_dir(pending_delete_dir);

    // Empty log file
    touch_file(pending_delete_dir + "/sstables-0-0.log");

    // Empty temporary log file
    touch_file(pending_delete_dir + "/sstables-1-1.log.tmp");

    const sstring toc_text = "TOC.txt\nData.db\n";

    // Regular log file with single entry
    write_file(gen_filename(2, component_type::TOC), toc_text);
    touch_file(gen_filename(2, component_type::Data));
    write_file(pending_delete_dir + "/sstables-2-2.log",
               component_basename(2, component_type::TOC) + "\n");

    // Temporary log file with single entry
    write_file(pending_delete_dir + "/sstables-3-3.log.tmp",
               component_basename(3, component_type::TOC) + "\n");

    // Regular log file with multiple entries
    write_file(gen_filename(4, component_type::TOC), toc_text);
    touch_file(gen_filename(4, component_type::Data));
    write_file(gen_filename(5, component_type::TOC), toc_text);
    touch_file(gen_filename(5, component_type::Data));
    write_file(pending_delete_dir + "/sstables-4-5.log",
               component_basename(4, component_type::TOC) + "\n" +
               component_basename(5, component_type::TOC) + "\n");

    // Regular log file with multiple entries and some deleted sstables
    write_file(gen_filename(6, component_type::TemporaryTOC), toc_text);
    touch_file(gen_filename(6, component_type::Data));
    write_file(gen_filename(7, component_type::TemporaryTOC), toc_text);
    write_file(pending_delete_dir + "/sstables-6-8.log",
               component_basename(6, component_type::TOC) + "\n" +
               component_basename(7, component_type::TOC) + "\n" +
               component_basename(8, component_type::TOC) + "\n");

    do_with_cql_env_thread([&] (cql_test_env& e) {
        // Empty log file
        require_exist(pending_delete_dir + "/sstables-0-0.log", false);

        // Empty temporary log file
        require_exist(pending_delete_dir + "/sstables-1-1.log.tmp", false);

        // Regular log file with single entry
        require_exist(gen_filename(2, component_type::TOC), false);
        require_exist(gen_filename(2, component_type::Data), false);
        require_exist(pending_delete_dir + "/sstables-2-2.log", false);

        // Temporary log file with single entry
        require_exist(pending_delete_dir + "/sstables-3-3.log.tmp", false);

        // Regular log file with multiple entries
        require_exist(gen_filename(4, component_type::TOC), false);
        require_exist(gen_filename(4, component_type::Data), false);
        require_exist(gen_filename(5, component_type::TOC), false);
        require_exist(gen_filename(5, component_type::Data), false);
        require_exist(pending_delete_dir + "/sstables-4-5.log", false);

        // Regular log file with multiple entries and some deleted sstables
        require_exist(gen_filename(6, component_type::TemporaryTOC), false);
        require_exist(gen_filename(6, component_type::Data), false);
        require_exist(gen_filename(7, component_type::TemporaryTOC), false);
        require_exist(pending_delete_dir + "/sstables-6-8.log", false);
    }, db_cfg_ptr).get();
}

// Snapshot tests and their helpers
future<> do_with_some_data(std::vector<sstring> cf_names, std::function<future<> (cql_test_env& env)> func, shared_ptr<db::config> db_cfg_ptr = {}) {
    return seastar::async([cf_names = std::move(cf_names), func = std::move(func), db_cfg_ptr = std::move(db_cfg_ptr)] () mutable {
        lw_shared_ptr<tmpdir> tmpdir_for_data;
        if (!db_cfg_ptr) {
            tmpdir_for_data = make_lw_shared<tmpdir>();
            db_cfg_ptr = make_shared<db::config>();
            db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
        }
        do_with_cql_env_thread([cf_names = std::move(cf_names), func = std::move(func)] (cql_test_env& e) {
            for (const auto& cf_name : cf_names) {
                e.create_table([&cf_name] (std::string_view ks_name) {
                    return *schema_builder(ks_name, cf_name)
                            .with_column("p1", utf8_type, column_kind::partition_key)
                            .with_column("c1", int32_type, column_kind::clustering_key)
                            .with_column("c2", int32_type, column_kind::clustering_key)
                            .with_column("r1", int32_type)
                            .build();
                }).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 1, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 2, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key1', 3, 2, 3);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 4, 5, 6);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 5, 5, 6);", cf_name)).get();
                e.execute_cql(fmt::format("insert into {} (p1, c1, c2, r1) values ('key2', 6, 5, 6);", cf_name)).get();
            }

            func(e).get();
        }, db_cfg_ptr).get();
    });
}

future<> take_snapshot(sharded<replica::database>& db, bool skip_flush = false, sstring ks_name = "ks", sstring cf_name = "cf", sstring snapshot_name = "test") {
    try {
        co_await replica::database::snapshot_table_on_all_shards(db, ks_name, cf_name, snapshot_name, db::snapshot_ctl::snap_views::no, skip_flush);
    } catch (...) {
        testlog.error("Could not take snapshot for {}.{} snapshot_name={} skip_flush={}: {}",
                ks_name, cf_name, snapshot_name, skip_flush, std::current_exception());
        throw;
    }
}

future<> take_snapshot(cql_test_env& e, bool skip_flush = false) {
    return take_snapshot(e.db(), skip_flush);
}

future<> take_snapshot(cql_test_env& e, sstring ks_name, sstring cf_name, sstring snapshot_name = "test") {
    return take_snapshot(e.db(), false /* skip_flush */, std::move(ks_name), std::move(cf_name), std::move(snapshot_name));
}

SEASTAR_TEST_CASE(snapshot_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e).get();

        std::set<sstring> expected = {
            "manifest.json",
        };

        auto& cf = e.local_db().find_column_family("ks", "cf");
        lister::scan_dir(fs::path(cf.dir()), { directory_entry_type::regular }, [&expected] (fs::path parent_dir, directory_entry de) {
            expected.insert(de.name);
            return make_ready_future<>();
        }).get();
        // snapshot triggered a flush and wrote the data down.
        BOOST_REQUIRE_GT(expected.size(), 1);

        // all files were copied and manifest was generated
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), { directory_entry_type::regular }, [&expected] (fs::path parent_dir, directory_entry de) {
            expected.erase(de.name);
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_EQUAL(expected.size(), 0);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_skip_flush_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e, true /* skip_flush */).get();

        std::set<sstring> expected = {
            "manifest.json",
        };

        auto& cf = e.local_db().find_column_family("ks", "cf");
        lister::scan_dir(fs::path(cf.dir()), { directory_entry_type::regular }, [&expected] (fs::path parent_dir, directory_entry de) {
            expected.insert(de.name);
            return make_ready_future<>();
        }).get();
        // Snapshot did not trigger a flush.
        // Only "manifest.json" is expected.
        BOOST_REQUIRE_EQUAL(expected.size(), 1);

        // all files were copied and manifest was generated
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), { directory_entry_type::regular }, [&expected] (fs::path parent_dir, directory_entry de) {
            expected.erase(de.name);
            return make_ready_future<>();
        }).get();

        BOOST_REQUIRE_EQUAL(expected.size(), 0);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_okay) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        lister::scan_dir(fs::path(cf.dir()), { directory_entry_type::regular }, [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get0().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_contains_dropped_tables) {
    return do_with_some_data({"cf1", "cf2", "cf3", "cf4"}, [] (cql_test_env& e) {
        e.execute_cql("DROP TABLE ks.cf1;").get();

        auto details = e.local_db().get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        const auto& sd = details.front().details;
        BOOST_REQUIRE_GT(sd.live, 0);
        BOOST_REQUIRE_EQUAL(sd.total, sd.live);

        take_snapshot(e, "ks", "cf2", "test2").get();
        take_snapshot(e, "ks", "cf3", "test3").get();

        details = e.local_db().get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 3);

        e.execute_cql("DROP TABLE ks.cf4;").get();

        details = e.local_db().get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 4);

        for (const auto& result : details) {
            const auto& sd = result.details;

            if (result.snapshot_name == "test2" || result.snapshot_name == "test3") {
                BOOST_REQUIRE_EQUAL(sd.live, 0);
                BOOST_REQUIRE_GT(sd.total, 0);
            } else {
                BOOST_REQUIRE_GT(sd.live, 0);
                BOOST_REQUIRE_EQUAL(sd.total, sd.live);
            }
        }

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(snapshot_list_inexistent) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto details = cf.get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 0);
        return make_ready_future<>();
    });
}


SEASTAR_TEST_CASE(clear_snapshot) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        take_snapshot(e).get();
        auto& cf = e.local_db().find_column_family("ks", "cf");

        unsigned count = 0;
        lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), { directory_entry_type::regular }, [&count] (fs::path parent_dir, directory_entry de) {
            count++;
            return make_ready_future<>();
        }).get();
        BOOST_REQUIRE_GT(count, 1); // expect more than the manifest alone

        e.local_db().clear_snapshot("test", {"ks"}, "").get();
        count = 0;

        BOOST_REQUIRE_EQUAL(fs::exists(fs::path(cf.dir()) / sstables::snapshots_dir / "test"), false);
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(clear_multiple_snapshots) {
    sstring ks_name = "ks";
    sstring table_name = "cf";
    auto num_snapshots = 2;

    auto snapshot_name = [] (int idx) {
        return format("test-snapshot-{}", idx);
    };

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) {
        auto& t = e.local_db().find_column_family(ks_name, table_name);
        auto table_dir = fs::path(t.dir());
        auto snapshots_dir = table_dir / sstables::snapshots_dir;

        for (auto i = 0; i < num_snapshots; i++) {
            testlog.debug("Taking snapshot {} on {}.{}", snapshot_name(i), ks_name, table_name);
            take_snapshot(e, ks_name, table_name, snapshot_name(i)).get();
        }

        for (auto i = 0; i < num_snapshots; i++) {
            unsigned count = 0;
            testlog.debug("Verifying {}", snapshots_dir / snapshot_name(i));
            lister::scan_dir(snapshots_dir / snapshot_name(i), { directory_entry_type::regular }, [&count] (fs::path parent_dir, directory_entry de) {
                count++;
                return make_ready_future<>();
            }).get();
            BOOST_REQUIRE_GT(count, 1); // expect more than the manifest alone
        }

        // non-existent tag
        testlog.debug("Clearing bogus tag");
        e.local_db().clear_snapshot("bogus", {ks_name}, table_name).get();
        for (auto i = 0; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), true);
        }

        // clear single tag
        testlog.debug("Clearing snapshot={} of {}.{}", snapshot_name(0), ks_name, table_name);
        e.local_db().clear_snapshot(snapshot_name(0), {ks_name}, table_name).get();
        BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(0)), false);
        for (auto i = 1; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), true);
        }

        // clear all tags (all tables)
        testlog.debug("Clearing all snapshots in {}", ks_name);
        e.local_db().clear_snapshot("", {ks_name}, "").get();
        for (auto i = 0; i < num_snapshots; i++) {
            BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(i)), false);
        }

        testlog.debug("Taking an extra {} of {}.{}", snapshot_name(num_snapshots), ks_name, table_name);
        take_snapshot(e, ks_name, table_name, snapshot_name(num_snapshots)).get();

        // existing snapshots expected to remain after dropping the table
        testlog.debug("Dropping table {}.{}", ks_name, table_name);
        replica::database::drop_table_on_all_shards(e.db(), ks_name, table_name).get();
        BOOST_REQUIRE_EQUAL(fs::exists(snapshots_dir / snapshot_name(num_snapshots)), true);

        // clear all tags
        testlog.debug("Clearing all snapshots in {}.{} after it had been dropped", ks_name, table_name);
        e.local_db().clear_snapshot("", {ks_name}, table_name).get();

        assert(!fs::exists(table_dir));

        // after all snapshots had been cleared,
        // the dropped table directory is expected to be removed.
        BOOST_REQUIRE_EQUAL(fs::exists(table_dir), false);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(clear_nonexistent_snapshot) {
    // no crashes, no exceptions
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        e.local_db().clear_snapshot("test", {"ks"}, "").get();
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_snapshot_ctl_details) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        sharded<db::snapshot_ctl> sc;
        sc.start(std::ref(e.db())).get();
        auto stop_sc = deferred_stop(sc);

        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        auto sc_details = sc.local().get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(sc_details.size(), 1);

        auto sc_sd_vec = sc_details["test"];
        BOOST_REQUIRE_EQUAL(sc_sd_vec.size(), 1);
        const auto &sc_sd = sc_sd_vec[0];
        BOOST_REQUIRE_EQUAL(sc_sd.ks, "ks");
        BOOST_REQUIRE_EQUAL(sc_sd.cf, "cf");
        BOOST_REQUIRE_EQUAL(sc_sd.live, sd.live);
        BOOST_REQUIRE_EQUAL(sc_sd.total, sd.total);

        lister::scan_dir(fs::path(cf.dir()), { directory_entry_type::regular }, [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get0().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        sc_details = sc.local().get_snapshot_details().get0();
        auto sc_sd_post_deletion_vec = sc_details["test"];
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion_vec.size(), 1);
        const auto &sc_sd_post_deletion = sc_sd_post_deletion_vec[0];
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.ks, "ks");
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.cf, "cf");
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.live, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sc_sd_post_deletion.total, sd_post_deletion.total);

        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_snapshot_ctl_true_snapshots_size) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) {
        sharded<db::snapshot_ctl> sc;
        sc.start(std::ref(e.db())).get();
        auto stop_sc = deferred_stop(sc);

        auto& cf = e.local_db().find_column_family("ks", "cf");
        take_snapshot(e).get();

        auto details = cf.get_snapshot_details().get0();
        BOOST_REQUIRE_EQUAL(details.size(), 1);

        auto sd = details["test"];
        BOOST_REQUIRE_EQUAL(sd.live, 0);
        BOOST_REQUIRE_GT(sd.total, 0);

        auto sc_live_size = sc.local().true_snapshots_size().get0();
        BOOST_REQUIRE_EQUAL(sc_live_size, sd.live);

        lister::scan_dir(fs::path(cf.dir()), { directory_entry_type::regular }, [] (fs::path parent_dir, directory_entry de) {
            fs::remove(parent_dir / de.name);
            return make_ready_future<>();
        }).get();

        auto sd_post_deletion = cf.get_snapshot_details().get0().at("test");

        BOOST_REQUIRE_EQUAL(sd_post_deletion.total, sd_post_deletion.live);
        BOOST_REQUIRE_EQUAL(sd.total, sd_post_deletion.live);

        sc_live_size = sc.local().true_snapshots_size().get0();
        BOOST_REQUIRE_EQUAL(sc_live_size, sd_post_deletion.live);

        return make_ready_future<>();
    });
}

// toppartitions_query caused a lw_shared_ptr to cross shards when moving results, #5104
SEASTAR_TEST_CASE(toppartitions_cross_shard_schema_ptr) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.tab (id int PRIMARY KEY)").get();
        db::toppartitions_query tq(e.db(), {{"ks", "tab"}}, {}, 1s, 100, 100);
        tq.scatter().get();
        auto q = e.prepare("INSERT INTO ks.tab(id) VALUES(?)").get0();
        // Generate many values to ensure crossing shards
        for (auto i = 0; i != 100; ++i) {
            e.execute_prepared(q, {cql3::raw_value::make_value(int32_type->decompose(i))}).get();
        }
        // This should trigger the bug in debug mode
        tq.gather().get();
    });
}

SEASTAR_THREAD_TEST_CASE(read_max_size) {
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk text, ck int, v text, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck, v) VALUES (?, ?, ?);").get0();

        auto& db = e.local_db();
        auto& tab = db.find_column_family("ks", "test");
        auto s = tab.schema();

        auto pk = make_local_key(s);
        const auto raw_pk = utf8_type->decompose(data_value(pk));
        const auto cql3_pk = cql3::raw_value::make_value(raw_pk);

        const auto value = sstring(1024, 'a');
        const auto raw_value = utf8_type->decompose(data_value(value));
        const auto cql3_value = cql3::raw_value::make_value(raw_value);

        const int num_rows = 1024;

        for (int i = 0; i != num_rows; ++i) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck, cql3_value}).get();
        }

        const auto partition_ranges = std::vector<dht::partition_range>{query::full_partition_range};

        const std::vector<std::pair<sstring, std::function<future<size_t>(schema_ptr, const query::read_command&)>>> query_methods{
                {"query_mutations()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query_mutations(s, cmd, partition_ranges.front(), {}, db::no_timeout).then(
                            [] (const std::tuple<reconcilable_result, cache_temperature>& res) {
                        return std::get<0>(res).memory_usage();
                    });
                }},
                {"query()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query(s, cmd, query::result_options::only_result(), partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<lw_shared_ptr<query::result>, cache_temperature>& res) {
                        return size_t(std::get<0>(res)->buf().size());
                    });
                }},
                {"query_mutations_on_all_shards()", [&e, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return query_mutations_on_all_shards(e.db(), s, cmd, partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>& res) {
                        return std::get<0>(res)->memory_usage();
                    });
                }}
        };

        for (auto [query_method_name, query_method] : query_methods) {
            for (auto allow_short_read : {true, false}) {
                for (auto max_size : {1024u, 1024u * 1024u, 1024u * 1024u * 1024u}) {
                    const auto should_throw = max_size < (num_rows * value.size() * 2) && !allow_short_read;
                    testlog.info("checking: query_method={}, allow_short_read={}, max_size={}, should_throw={}", query_method_name, allow_short_read, max_size, should_throw);
                    auto slice = s->full_slice();
                    if (allow_short_read) {
                        slice.options.set<query::partition_slice::option::allow_short_read>();
                    } else {
                        slice.options.remove<query::partition_slice::option::allow_short_read>();
                    }
                    query::read_command cmd(s->id(), s->version(), slice, query::max_result_size(max_size), query::tombstone_limit::max);
                    try {
                        auto size = query_method(s, cmd).get0();
                        // Just to ensure we are not interpreting empty results as success.
                        BOOST_REQUIRE(size != 0);
                        if (should_throw) {
                            BOOST_FAIL("Expected exception, but none was thrown.");
                        } else {
                            testlog.trace("No exception thrown, as expected.");
                        }
                    } catch (std::runtime_error& e) {
                        if (should_throw) {
                            testlog.trace("Exception thrown, as expected: {}", e);
                        } else {
                            BOOST_FAIL(fmt::format("Expected no exception, but caught: {}", e));
                        }
                    }
                }
            }
        }
    }).get();
}

// Check that mutation queries, those that are stopped when the memory
// consumed by their results reach the local/global limit, are aborted
// instead of silently terminated when this happens.
SEASTAR_THREAD_TEST_CASE(unpaged_mutation_read_global_limit) {
    auto cfg = cql_test_config{};
    cfg.dbcfg.emplace();
    // The memory available to the result memory limiter (global limit) is
    // configured based on the available memory, so give a small amount to
    // the "node", so we don't have to work with large amount of data.
    cfg.dbcfg->available_memory = 2 * 1024 * 1024;
    do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (pk text, ck int, v text, PRIMARY KEY (pk, ck));").get();
        auto id = e.prepare("INSERT INTO test (pk, ck, v) VALUES (?, ?, ?);").get0();

        auto& db = e.local_db();
        auto& tab = db.find_column_family("ks", "test");
        auto s = tab.schema();

        auto pk = make_local_key(s);
        const auto raw_pk = utf8_type->decompose(data_value(pk));
        const auto cql3_pk = cql3::raw_value::make_value(raw_pk);

        const auto value = sstring(1024, 'a');
        const auto raw_value = utf8_type->decompose(data_value(value));
        const auto cql3_value = cql3::raw_value::make_value(raw_value);

        const int num_rows = 1024;
        const auto max_size = 1024u * 1024u * 1024u;

        for (int i = 0; i != num_rows; ++i) {
            const auto cql3_ck = cql3::raw_value::make_value(int32_type->decompose(data_value(i)));
            e.execute_prepared(id, {cql3_pk, cql3_ck, cql3_value}).get();
        }

        const auto partition_ranges = std::vector<dht::partition_range>{query::full_partition_range};

        const std::vector<std::pair<sstring, std::function<future<size_t>(schema_ptr, const query::read_command&)>>> query_methods{
                {"query_mutations()", [&db, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return db.query_mutations(s, cmd, partition_ranges.front(), {}, db::no_timeout).then(
                            [] (const std::tuple<reconcilable_result, cache_temperature>& res) {
                        return std::get<0>(res).memory_usage();
                    });
                }},
                {"query_mutations_on_all_shards()", [&e, &partition_ranges] (schema_ptr s, const query::read_command& cmd) -> future<size_t> {
                    return query_mutations_on_all_shards(e.db(), s, cmd, partition_ranges, {}, db::no_timeout).then(
                            [] (const std::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>& res) {
                        return std::get<0>(res)->memory_usage();
                    });
                }}
        };

        for (auto [query_method_name, query_method] : query_methods) {
            testlog.info("checking: query_method={}", query_method_name);
            auto slice = s->full_slice();
            slice.options.remove<query::partition_slice::option::allow_short_read>();
            query::read_command cmd(s->id(), s->version(), slice, query::max_result_size(max_size), query::tombstone_limit::max);
            try {
                auto size = query_method(s, cmd).get0();
                // Just to ensure we are not interpreting empty results as success.
                BOOST_REQUIRE(size != 0);
                BOOST_FAIL("Expected exception, but none was thrown.");
            } catch (std::runtime_error& e) {
                testlog.trace("Exception thrown, as expected: {}", e);
            }
        }
    }, std::move(cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(reader_concurrency_semaphore_selection_test) {
    cql_test_config cfg;

    scheduling_group unknown_scheduling_group = create_scheduling_group("unknown", 800).get();
    auto cleanup_unknown_scheduling_group = defer([&unknown_scheduling_group] {
        destroy_scheduling_group(unknown_scheduling_group).get();
    });

    const auto user_semaphore = std::mem_fn(&database_test::get_user_read_concurrency_semaphore);
    const auto system_semaphore = std::mem_fn(&database_test::get_system_read_concurrency_semaphore);
    const auto streaming_semaphore = std::mem_fn(&database_test::get_streaming_read_concurrency_semaphore);

    std::vector<std::pair<scheduling_group, std::function<reader_concurrency_semaphore&(database_test&)>>> scheduling_group_and_expected_semaphore{
        {default_scheduling_group(), system_semaphore}
    };

    auto sched_groups = get_scheduling_groups().get();

    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.compaction_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memory_compaction_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.streaming_scheduling_group, streaming_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.statement_scheduling_group, user_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memtable_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.memtable_to_cache_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(sched_groups.gossip_scheduling_group, system_semaphore);
    scheduling_group_and_expected_semaphore.emplace_back(unknown_scheduling_group, user_semaphore);

    do_with_cql_env_thread([&scheduling_group_and_expected_semaphore] (cql_test_env& e) {
        auto& db = e.local_db();

        database_test tdb(db);
        for (const auto& [sched_group, expected_sem_getter] : scheduling_group_and_expected_semaphore) {
            with_scheduling_group(sched_group, [&db, sched_group = sched_group, &tdb, &expected_sem_getter = expected_sem_getter] {
                auto expected_sem_ptr = &expected_sem_getter(tdb);
                auto& sem = db.get_reader_concurrency_semaphore();
                if (&sem != expected_sem_ptr) {
                    BOOST_FAIL(fmt::format("Unexpected semaphore for scheduling group {}, expected {}, got {}", sched_group.name(), expected_sem_ptr->name(), sem.name()));
                }
            }).get();
        }
    }, std::move(cfg)).get();
}

SEASTAR_THREAD_TEST_CASE(max_result_size_for_unlimited_query_selection_test) {
    cql_test_config cfg;

    cfg.db_config->max_memory_for_unlimited_query_soft_limit(1 * 1024 * 1024, utils::config_file::config_source::CommandLine);
    cfg.db_config->max_memory_for_unlimited_query_hard_limit(2 * 1024 * 1024, utils::config_file::config_source::CommandLine);

    scheduling_group unknown_scheduling_group = create_scheduling_group("unknown", 800).get();
    auto cleanup_unknown_scheduling_group = defer([&unknown_scheduling_group] {
        destroy_scheduling_group(unknown_scheduling_group).get();
    });

    const auto user_max_result_size = query::max_result_size(cfg.db_config->max_memory_for_unlimited_query_soft_limit(),
            cfg.db_config->max_memory_for_unlimited_query_hard_limit());
    const auto system_max_result_size = query::max_result_size(query::result_memory_limiter::unlimited_result_size);
    const auto maintenance_max_result_size = system_max_result_size;

    std::vector<std::pair<scheduling_group, query::max_result_size>> scheduling_group_and_expected_max_result_size{
        {default_scheduling_group(), system_max_result_size}
    };

    auto sched_groups = get_scheduling_groups().get();

    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.compaction_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memory_compaction_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.streaming_scheduling_group, maintenance_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.statement_scheduling_group, user_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memtable_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.memtable_to_cache_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(sched_groups.gossip_scheduling_group, system_max_result_size);
    scheduling_group_and_expected_max_result_size.emplace_back(unknown_scheduling_group, user_max_result_size);

    do_with_cql_env_thread([&scheduling_group_and_expected_max_result_size] (cql_test_env& e) {
        auto& db = e.local_db();
        database_test tdb(db);
        for (const auto& [sched_group, expected_max_size] : scheduling_group_and_expected_max_result_size) {
            with_scheduling_group(sched_group, [&db, sched_group = sched_group, expected_max_size = expected_max_size] {
                const auto max_size = db.get_unlimited_query_max_result_size();
                if (max_size != expected_max_size) {
                    BOOST_FAIL(fmt::format("Unexpected max_size for scheduling group {}, expected {{{}, {}}}, got {{{}, {}}}",
                                sched_group.name(),
                                expected_max_size.soft_limit,
                                expected_max_size.hard_limit,
                                max_size.soft_limit,
                                max_size.hard_limit));
                }
            }).get();
        }
    }, std::move(cfg)).get();
}

// Test `upgrade_sstables` on all keyspaces (including the system keyspace).
// Refs: #9494 (https://github.com/scylladb/scylla/issues/9494)
SEASTAR_TEST_CASE(upgrade_sstables) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.db().invoke_on_all([&e] (replica::database& db) -> future<> {
            auto& cm = db.get_compaction_manager();
            for (auto& [ks_name, ks] : db.get_keyspaces()) {
                auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(db.get_keyspace_local_ranges(ks_name));
                for (auto& [cf_name, schema] : ks.metadata()->cf_meta_data()) {
                    auto& t = db.find_column_family(schema->id());
                    constexpr bool exclude_current_version = false;
                    co_await cm.perform_sstable_upgrade(owned_ranges_ptr, t.as_table_state(), exclude_current_version);
                }
            }
        }).get();
    });
}

SEASTAR_THREAD_TEST_CASE(per_service_level_reader_concurrency_semaphore_test) {
    cql_test_config cfg;
    do_with_cql_env_thread([] (cql_test_env& e) {
        const size_t num_service_levels = 3;
        const size_t num_keys_to_insert = 10;
        const size_t num_individual_reads_to_test = 50;
        auto& db = e.local_db();
        database_test dbt(db);
        size_t total_memory = dbt.get_total_user_reader_concurrency_semaphore_memory();
        sharded<qos::service_level_controller>& sl_controller = e.service_level_controller_service();
        std::array<sstring, num_service_levels> sl_names;
        qos::service_level_options slo;
        size_t expected_total_weight = 0;
        auto index_to_weight = [] (size_t i) -> size_t {
            return (i + 1)*100;
        };

        // make the default service level take as little memory as possible
        slo.shares.emplace<int32_t>(1);
        expected_total_weight += 1;
        sl_controller.local().add_service_level(qos::service_level_controller::default_service_level_name, slo).get();

        // Just to make the code more readable.
        auto get_reader_concurrency_semaphore_for_sl = [&] (sstring sl_name) -> reader_concurrency_semaphore& {
            return *sl_controller.local().with_service_level(sl_name, noncopyable_function<reader_concurrency_semaphore*()>([&] {
                return &db.get_reader_concurrency_semaphore();
            })).get0();
        };

        for (int i = 0; i < num_service_levels; i++) {
            sstring sl_name = format("sl{}", i);
            slo.shares.emplace<int32_t>(index_to_weight(i));
            sl_controller.local().add_service_level(sl_name, slo).get();
            expected_total_weight += index_to_weight(i);
            // Make sure that the total weight is tracked correctly in the semaphore group
            BOOST_REQUIRE_EQUAL(expected_total_weight, dbt.get_total_user_reader_concurrency_semaphore_weight());
            sl_names[i] = sl_name;
            size_t total_distributed_memory = 0;
            for (int j = 0 ; j <= i ; j++) {
                reader_concurrency_semaphore& sem = get_reader_concurrency_semaphore_for_sl(sl_names[j]);
                // Make sure that all semaphores that has been created until now - have the right amount of available memory
                // after the operation has ended.
                // We allow for a small delta of up to num_service_levels. This allows an off-by-one for each semaphore,
                // the remainder being added to one of the semaphores.
                // We make sure this didn't leak/create memory by checking the total below.
                const auto delta = std::abs(ssize_t((index_to_weight(j) * total_memory) / expected_total_weight) - sem.available_resources().memory);
                BOOST_REQUIRE_LE(delta, num_service_levels);
                total_distributed_memory += sem.available_resources().memory;
            }
            total_distributed_memory += get_reader_concurrency_semaphore_for_sl(qos::service_level_controller::default_service_level_name).available_resources().memory;
            BOOST_REQUIRE_EQUAL(total_distributed_memory, total_memory);
        }

        size_t total_weight = dbt.get_total_user_reader_concurrency_semaphore_weight();
        auto get_semaphores_stats_snapshot = [&] () {
            std::unordered_map<sstring, reader_concurrency_semaphore::stats> snapshot;
            for (auto&& sl_name : sl_names) {
                snapshot[sl_name] = get_reader_concurrency_semaphore_for_sl(sl_name).get_stats();
            }
            return snapshot;
        };
        e.execute_cql("CREATE TABLE tbl (a int, b int, PRIMARY KEY (a));").get();

        for (int i = 0; i < num_keys_to_insert; i++) {
            for (int j = 0; j < num_keys_to_insert; j++) {
                e.execute_cql(format("INSERT INTO tbl(a, b) VALUES ({}, {});", i, j)).get();
            }
        }

        for (int i = 0; i < num_individual_reads_to_test; i++) {
            int random_service_level = tests::random::get_int(num_service_levels - 1);
            auto snapshot_before = get_semaphores_stats_snapshot();

            sl_controller.local().with_service_level(sl_names[random_service_level], noncopyable_function<future<>()> ([&] {
                return e.execute_cql("SELECT * FROM tbl;").discard_result();
            })).get();
            auto snapshot_after = get_semaphores_stats_snapshot();
            for (auto& [sl_name, stats] : snapshot_before) {
                // Make sure that the only semaphore that experienced any activity (at least measured activity) is
                // the semaphore that belongs to the current service level.
                BOOST_REQUIRE((stats == snapshot_after[sl_name] && sl_name != sl_names[random_service_level]) ||
                        (stats != snapshot_after[sl_name] && sl_name == sl_names[random_service_level]));
            }
        }
    }, std::move(cfg)).get();
}

SEASTAR_TEST_CASE(populate_from_quarantine_works) {
    auto tmpdir_for_data = make_lw_shared<tmpdir>();
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
    locator::host_id host_id;

    // populate tmpdir_for_data and
    // move a random sstable to quarantine
    co_await do_with_some_data({"cf"}, [&host_id] (cql_test_env& e) -> future<> {
        host_id = e.local_db().get_config().host_id;
        auto& db = e.db();
        co_await db.invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.flush();
        });
        auto shard = tests::random::get_int<unsigned>(0, smp::count);
        auto found = false;
        for (auto i = 0; i < smp::count && !found; i++) {
            found = co_await db.invoke_on((shard + i) % smp::count, [] (replica::database& db) -> future<bool> {
                auto& cf = db.find_column_family("ks", "cf");
                auto sstables = in_strategy_sstables(cf.as_table_state());
                if (sstables.empty()) {
                    co_return false;
                }
                auto idx = tests::random::get_int<size_t>(0, sstables.size() - 1);
                testlog.debug("Moving sstable #{} out of {} to quarantine", idx, sstables.size());
                auto sst = sstables[idx];
                auto quarantine_dir = sst->get_dir() + "/" + sstables::quarantine_dir;
                co_await touch_directory(quarantine_dir);
                co_await sst->move_to_new_dir(quarantine_dir, sst->generation());
                co_return true;
            });
        }
        BOOST_REQUIRE(found);
    }, db_cfg_ptr);

    // reload the table from tmpdir_for_data and
    // verify that all rows are still there
    db_cfg_ptr->host_id = host_id;
    size_t row_count = 0;
    co_await do_with_cql_env([&row_count] (cql_test_env& e) -> future<> {
        auto res = co_await e.execute_cql("select * from ks.cf;");
        auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(res);
        BOOST_REQUIRE(rows);
        row_count = rows->rs().result_set().size();
    }, std::move(db_cfg_ptr));
    BOOST_REQUIRE_EQUAL(row_count, 6);
}

SEASTAR_TEST_CASE(snapshot_with_quarantine_works) {
    return do_with_some_data({"cf"}, [] (cql_test_env& e) -> future<> {
        auto& db = e.db();
        co_await db.invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.flush();
        });

        std::set<sstring> expected = {
            "manifest.json",
        };

        // move a random sstable to quarantine
        auto shard = tests::random::get_int<unsigned>(0, smp::count);
        auto found = false;
        for (auto i = 0; i < smp::count; i++) {
            co_await db.invoke_on((shard + i) % smp::count, [&] (replica::database& db) -> future<> {
                auto& cf = db.find_column_family("ks", "cf");
                auto sstables = in_strategy_sstables(cf.as_table_state());
                if (sstables.empty()) {
                    co_return;
                }
                // collect all expected sstable data files
                for (auto sst : sstables) {
                    expected.insert(fs::path(sst->get_filename()).filename().native());
                }
                if (std::exchange(found, true)) {
                    co_return;
                }
                auto idx = tests::random::get_int<size_t>(0, sstables.size() - 1);
                auto sst = sstables[idx];
                auto quarantine_dir = sst->get_dir() + "/" + sstables::quarantine_dir;
                co_await touch_directory(quarantine_dir);
                testlog.debug("Moving sstable #{} out of {}: {} to quarantine", idx, sstables.size(), sst->get_filename());
                co_await sst->move_to_new_dir(quarantine_dir, sst->generation());
            });
        }
        BOOST_REQUIRE(found);

        co_await take_snapshot(db, true /* skip_flush */);

        testlog.debug("Expected: {}", expected);

        // snapshot triggered a flush and wrote the data down.
        BOOST_REQUIRE_GT(expected.size(), 1);

        auto& cf = db.local().find_column_family("ks", "cf");

        // all files were copied and manifest was generated
        co_await lister::scan_dir((fs::path(cf.dir()) / sstables::snapshots_dir / "test"), { directory_entry_type::regular }, [&expected] (fs::path parent_dir, directory_entry de) {
            testlog.debug("Found in snapshots: {}", de.name);
            expected.erase(de.name);
            return make_ready_future<>();
        });

        if (!expected.empty()) {
            testlog.error("Not in snapshots: {}", expected);
        }

        BOOST_REQUIRE(expected.empty());
    });
}

SEASTAR_TEST_CASE(database_drop_column_family_clears_querier_cache) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table ks.cf (k text, v int, primary key (k));").get();
        auto& db = e.local_db();
        const auto ts = db_clock::now();
        auto& tbl = db.find_column_family("ks", "cf");

        auto op = std::optional(tbl.read_in_progress());
        auto s = tbl.schema();
        auto q = query::querier(
                tbl.as_mutation_source(),
                tbl.schema(),
                database_test(db).get_user_read_concurrency_semaphore().make_tracking_only_permit(s.get(), "test", db::no_timeout),
                query::full_partition_range,
                s->full_slice(),
                default_priority_class(),
                nullptr);

        auto f = replica::database::drop_table_on_all_shards(e.db(), "ks", "cf");

        // we add a querier to the querier cache while the drop is ongoing
        auto& qc = db.get_querier_cache();
        qc.insert_data_querier(query_id::create_random_id(), std::move(q), nullptr);
        BOOST_REQUIRE_EQUAL(qc.get_stats().population, 1);

        op.reset(); // this should allow the drop to finish
        f.get();

        // the drop should have cleaned up all entries belonging to that table
        BOOST_REQUIRE_EQUAL(qc.get_stats().population, 0);
    });
}

static future<> test_drop_table_with_auto_snapshot(bool auto_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = format("table_with_auto_snapshot_{}", auto_snapshot ? "enabled" : "disabled");
    auto tmpdir_for_data = make_lw_shared<tmpdir>();
    auto db_cfg_ptr = make_shared<db::config>();
    db_cfg_ptr->data_file_directories(std::vector<sstring>({ tmpdir_for_data->path().string() }));
    db_cfg_ptr->auto_snapshot(auto_snapshot);

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // Pass `with_snapshot=true` to drop_table_on_all
        // to allow auto_snapshot (based on the configuration above).
        // The table directory should therefore exist after the table is dropped if auto_snapshot is disabled in the configuration.
        co_await replica::database::drop_table_on_all_shards(e.db(), ks_name, table_name, true);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, auto_snapshot);
        co_return;
    }, db_cfg_ptr);
}

SEASTAR_TEST_CASE(drop_table_with_auto_snapshot_enabled) {
    return test_drop_table_with_auto_snapshot(true);
}

SEASTAR_TEST_CASE(drop_table_with_auto_snapshot_disabled) {
    return test_drop_table_with_auto_snapshot(false);
}

SEASTAR_TEST_CASE(drop_table_with_no_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = "table_with_no_snapshot";

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // Pass `with_snapshot=false` to drop_table_on_all
        // to disallow auto_snapshot.
        // The table directory should therefore not exist after the table is dropped.
        co_await replica::database::drop_table_on_all_shards(e.db(), ks_name, table_name, false);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, false);
        co_return;
    });
}

SEASTAR_TEST_CASE(drop_table_with_explicit_snapshot) {
    sstring ks_name = "ks";
    sstring table_name = "table_with_explicit_snapshot";

    co_await do_with_some_data({table_name}, [&] (cql_test_env& e) -> future<> {
        auto snapshot_tag = format("test-{}", db_clock::now().time_since_epoch().count());
        co_await replica::database::snapshot_table_on_all_shards(e.db(), ks_name, table_name, snapshot_tag, db::snapshot_ctl::snap_views::no, false);
        auto cf_dir = e.local_db().find_column_family(ks_name, table_name).dir();

        // With explicit snapshot and with_snapshot=false
        // dir should still be kept, regardless of the
        // with_snapshot parameter and auto_snapshot config.
        co_await replica::database::drop_table_on_all_shards(e.db(), ks_name, table_name, false);
        auto cf_dir_exists = co_await file_exists(cf_dir);
        BOOST_REQUIRE_EQUAL(cf_dir_exists, true);
        co_return;
    });
}
