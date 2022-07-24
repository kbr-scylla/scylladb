/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/distributed.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "sstables/sstables.hh"
#include "compaction/incremental_compaction_strategy.hh"
#include "schema.hh"
#include "replica/database.hh"
#include "compaction/compaction_manager.hh"
#include "sstable_test.hh"
#include "sstables/metadata_collector.hh"
#include "test/lib/tmpdir.hh"
#include "cell_locking.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "service/storage_proxy.hh"
#include "test/lib/sstable_run_based_compaction_strategy_for_tests.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "db/large_data_handler.hh"
#include "db/config.hh"

#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"

using namespace sstables;

static flat_mutation_reader_v2 sstable_reader(reader_permit permit, shared_sstable sst, schema_ptr s) {
    return sst->as_mutation_source().make_reader_v2(s, std::move(permit), query::full_partition_range, s->full_slice());

}

class table_state_for_test : public table_state {
    test_env& _env;
    replica::table* _t;
public:
    explicit table_state_for_test(test_env& env, replica::table& t)
        : _env(env)
        , _t(&t)
    {
    }
    const schema_ptr& schema() const noexcept override {
        return _t->schema();
    }
    unsigned min_compaction_threshold() const noexcept override {
        return _t->schema()->min_compaction_threshold();
    }
    bool compaction_enforce_min_threshold() const noexcept override {
        return true;
    }
    const sstables::sstable_set& main_sstable_set() const override {
        return _t->get_sstable_set();
    }
    const sstables::sstable_set& maintenance_sstable_set() const override {
        return _t->maintenance_sstable_set();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point compaction_time) const override {
        return sstables::get_fully_expired_sstables(*this, sstables, compaction_time - schema()->gc_grace_seconds());
    }

    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override {
        return _t->compacted_undeleted_sstables();
    }
    sstables::compaction_strategy& get_compaction_strategy() const noexcept override {
        return _t->get_compaction_strategy();
    }
    reader_permit make_compaction_reader_permit() const override {
        return _t->compaction_concurrency_semaphore().make_tracking_only_permit(schema().get(), "compaction", db::no_timeout);
    }
    sstables::sstable_writer_config configure_writer(sstring origin) const override {
        return _t->get_sstables_manager().configure_writer(std::move(origin));
    }
    api::timestamp_type min_memtable_timestamp() const override {
        return api::max_timestamp;
    }
    future<> update_compaction_history(utils::UUID compaction_id, sstring ks_name, sstring cf_name, std::chrono::milliseconds ended_at, int64_t bytes_in, int64_t bytes_out)  override {
        return make_ready_future<>();
    }
    future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        return _t->as_table_state().on_compaction_completion(std::move(desc), offstrategy);
    }
    bool is_auto_compaction_disabled_by_user() const noexcept override {
        return false;
    }
    sstables::sstables_manager& get_sstables_manager() noexcept override {
        return _env.manager();
    }
    sstables::shared_sstable make_sstable() const override {
        return _t->make_sstable();
    }
};

static std::unique_ptr<table_state> make_table_state_for_test(test_env& env, replica::column_family& t) {
    return std::make_unique<table_state_for_test>(env, t);
}

class strategy_control_for_test : public strategy_control {
    bool _has_ongoing_compaction;
public:
    explicit strategy_control_for_test(bool has_ongoing_compaction) noexcept : _has_ongoing_compaction(has_ongoing_compaction) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return _has_ongoing_compaction;
    }
};

static std::unique_ptr<strategy_control> make_strategy_control_for_test(bool has_ongoing_compaction) {
    return std::make_unique<strategy_control_for_test>(has_ongoing_compaction);
}

SEASTAR_TEST_CASE(incremental_compaction_test) {
    return sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        cell_locker_stats cl_stats;

        auto builder = schema_builder("tests", "incremental_compaction_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type)
                .with_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
                .with_sharder(smp::count, 0);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [&env, s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            auto sst = env.make_sstable(s, tmp->path().string(), (*gen)++, sstable_version_types::md, big);
            return sst;
        };

        auto cm = make_lw_shared<compaction_manager>(compaction_manager::for_testing_tag());
        auto cmt = compaction_manager_test(*cm);
        auto tracker = make_lw_shared<cache_tracker>();
        auto cf = make_lw_shared<replica::column_family>(s, column_family_test_config(env.semaphore()), replica::column_family::no_commitlog(), *cm, env.manager(), cl_stats, *tracker);
        auto table_s = make_table_state_for_test(env, *cf);
        cf->mark_ready_for_writes();
        cf->start();
        cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
        auto compact = [&, s] (std::vector<shared_sstable> all, auto replacer) -> std::vector<shared_sstable> {
            auto desc = sstables::compaction_descriptor(std::move(all), service::get_local_compaction_priority(), 1, 0);
            desc.enable_garbage_collection(cf->get_sstable_set());
            return compact_sstables(*cm, std::move(desc), *cf, sst_gen, replacer).get0().new_sstables;
        };
        auto make_insert = [&] (auto p) {
            auto key = partition_key::from_exploded(*s, {to_bytes(p.first)});
            mutation m(s, key);
            m.set_clustered_cell(clustering_key::make_empty(), bytes("value"), data_value(int32_t(1)), 1 /* ts */);
            BOOST_REQUIRE(m.decorated_key().token() == p.second);
            return m;
        };

        auto tokens = token_generation_for_current_shard(16);
        std::unordered_set<shared_sstable> sstables;
        std::vector<utils::observer<sstable&>> observers;
        sstables::sstable_run_based_compaction_strategy_for_tests cs;

        auto do_replace = [&] (const std::vector<shared_sstable>& old_sstables, const std::vector<shared_sstable>& new_sstables) {
            for (auto& old_sst : old_sstables) {
                BOOST_REQUIRE(sstables.count(old_sst));
                sstables.erase(old_sst);
            }
            for (auto& new_sst : new_sstables) {
                BOOST_REQUIRE(!sstables.count(new_sst));
                sstables.insert(new_sst);
            }
            column_family_test(cf).rebuild_sstable_list(new_sstables, old_sstables);
            cmt.propagate_replacement(&*cf, old_sstables, new_sstables);
        };

        auto do_incremental_replace = [&] (auto old_sstables, auto new_sstables, auto& expected_sst, auto& closed_sstables_tracker) {
            // that's because each sstable will contain only 1 mutation.
            BOOST_REQUIRE(old_sstables.size() == 1);
            BOOST_REQUIRE(new_sstables.size() == 1);
            // check that sstable replacement follows token order
            BOOST_REQUIRE(*expected_sst == old_sstables.front()->generation());
            expected_sst++;
            // check that previously released sstables were already closed
            if (old_sstables.front()->generation().value() % 4 == 0) {
                // Due to performance reasons, sstables are not released immediately, but in batches.
                // At the time of writing, mutation_reader_merger releases it's sstable references
                // in batches of 4. That's why we only perform this check every 4th sstable. 
                BOOST_REQUIRE(*closed_sstables_tracker == old_sstables.front()->generation());
            }

            do_replace(old_sstables, new_sstables);

            observers.push_back(old_sstables.front()->add_on_closed_handler([&] (sstable& sst) {
                BOOST_TEST_MESSAGE(fmt::format("Closing sstable of generation {}", sst.generation()));
                closed_sstables_tracker++;
            }));

            BOOST_TEST_MESSAGE(fmt::format("Removing sstable of generation {}, refcnt: {}", old_sstables.front()->generation(), old_sstables.front().use_count()));
        };

        auto do_compaction = [&] (size_t expected_input, size_t expected_output) -> std::vector<shared_sstable> {
            auto input_ssts = std::vector<shared_sstable>(sstables.begin(), sstables.end());
            auto control = make_strategy_control_for_test(false);
            auto desc = cs.get_sstables_for_compaction(*table_s, *control, std::move(input_ssts));

            // nothing to compact, move on.
            if (desc.sstables.empty()) {
                return {};
            }
            std::unordered_set<utils::UUID> run_ids;
            bool incremental_enabled = std::any_of(desc.sstables.begin(), desc.sstables.end(), [&run_ids] (shared_sstable& sst) {
                return !run_ids.insert(sst->run_identifier()).second;
            });

            BOOST_REQUIRE(desc.sstables.size() == expected_input);
            auto sstable_run = boost::copy_range<std::set<sstables::generation_type>>(desc.sstables
                | boost::adaptors::transformed([] (auto& sst) { return sst->generation(); }));
            auto expected_sst = sstable_run.begin();
            auto closed_sstables_tracker = sstable_run.begin();
            auto replacer = [&] (compaction_completion_desc ccd) {
                BOOST_REQUIRE(expected_sst != sstable_run.end());
                if (incremental_enabled) {
                    do_incremental_replace(std::move(ccd.old_sstables), std::move(ccd.new_sstables), expected_sst, closed_sstables_tracker);
                } else {
                    do_replace(std::move(ccd.old_sstables), std::move(ccd.new_sstables));
                    expected_sst = sstable_run.end();
                }
            };

            auto result = compact(std::move(desc.sstables), replacer);

            BOOST_REQUIRE_EQUAL(expected_output, result.size());
            BOOST_REQUIRE(expected_sst == sstable_run.end());
            return result;
        };

        // Generate 4 sstable runs composed of 4 fragments each after 4 compactions.
        // All fragments non-overlapping.
        for (auto i = 0U; i < tokens.size(); i++) {
            auto sst = make_sstable_containing(sst_gen, { make_insert(tokens[i]) });
            sst->set_sstable_level(1);
            BOOST_REQUIRE(sst->get_sstable_level() == 1);
            column_family_test(cf).add_sstable(sst);
            sstables.insert(std::move(sst));
            do_compaction(4, 4);
        }
        BOOST_REQUIRE(sstables.size() == 16);

        // Generate 1 sstable run from 4 sstables runs of similar size
        auto result = do_compaction(16, 16);
        BOOST_REQUIRE(result.size() == 16);
        for (auto i = 0U; i < tokens.size(); i++) {
            assert_that(sstable_reader(env.semaphore().make_tracking_only_permit(s.get(), "test reader", db::no_timeout), result[i], s))
                .produces(make_insert(tokens[i]))
                .produces_end_of_stream();
        }
    });
}

SEASTAR_THREAD_TEST_CASE(incremental_compaction_sag_test) {
    auto builder = schema_builder("tests", "incremental_compaction_test")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
    auto s = builder.build();

    struct sag_test {
        test_env& _env;
        compaction_manager _cm { compaction_manager::for_testing_tag() };
        cell_locker_stats _cl_stats;
        cache_tracker _tracker;
        lw_shared_ptr<replica::column_family> _cf;
        incremental_compaction_strategy _ics;
        const unsigned min_threshold = 4;
        const size_t data_set_size = 1'000'000'000;

        static incremental_compaction_strategy make_ics(double space_amplification_goal) {
            std::map<sstring, sstring> options;
            options.emplace(sstring("space_amplification_goal"), sstring(std::to_string(space_amplification_goal)));
            return incremental_compaction_strategy(options);
        }
        static replica::column_family::config make_table_config(test_env& env) {
            auto config = column_family_test_config(env.semaphore());
            config.compaction_enforce_min_threshold = true;
            return config;
        }

        sag_test(test_env& env, schema_ptr s, double space_amplification_goal)
            : _env(env)
            , _cf(make_lw_shared<replica::column_family>(s, make_table_config(_env), replica::column_family::no_commitlog(), _cm, env.manager(), _cl_stats, _tracker))
            , _ics(make_ics(space_amplification_goal))
        {
        }

        double space_amplification() const {
            auto sstables = _cf->get_sstables();
            auto total = boost::accumulate(*sstables | boost::adaptors::transformed(std::mem_fn(&sstable::data_size)), uint64_t(0));
            return double(total) / data_set_size;
        }

        shared_sstable make_sstable_with_size(size_t sstable_data_size) {
            static thread_local unsigned gen = 0;
            auto sst = _env.make_sstable(_cf->schema(), "", gen++, sstable_version_types::md, big);
            sstables::test(sst).set_data_file_size(sstable_data_size);
            sstables::test(sst).set_values("z", "a", stats_metadata{});
            return sst;
        }

        void populate(double target_space_amplification) {
            auto add_sstable = [this] (unsigned sst_data_size) {
                auto sst = make_sstable_with_size(sst_data_size);
                column_family_test(_cf).add_sstable(sst);
            };

            add_sstable(data_set_size);
            while (space_amplification() < target_space_amplification) {
                add_sstable(data_set_size / min_threshold);
            }
        }

        void run() {
            auto table_s = make_table_state_for_test(_env, *_cf);
            auto control = make_strategy_control_for_test(false);
            for (;;) {
                auto desc = _ics.get_sstables_for_compaction(*table_s, *control, in_strategy_sstables(_cf->as_table_state()));
                // no more jobs, bailing out...
                if (desc.sstables.empty()) {
                    break;
                }
                auto total = boost::accumulate(desc.sstables | boost::adaptors::transformed(std::mem_fn(&sstable::data_size)), uint64_t(0));
                std::vector<shared_sstable> new_ssts = { make_sstable_with_size(std::min(total, data_set_size)) };
                column_family_test(_cf).rebuild_sstable_list(new_ssts, desc.sstables);
            }
        }
    };

    using SAG = double;
    using TABLE_INITIAL_SA = double;

    auto with_sag_test = [&] (SAG sag, TABLE_INITIAL_SA initial_sa) {
      test_env::do_with_async([&] (test_env& env) {
        sag_test test(env, s, sag);
        test.populate(initial_sa);
        BOOST_REQUIRE(test.space_amplification() >= initial_sa);
        test.run();
        BOOST_REQUIRE(test.space_amplification() <= sag);
      }).get();
    };

    with_sag_test(SAG(1.25), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(2), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(1.5), TABLE_INITIAL_SA(1.75));
    with_sag_test(SAG(1.01), TABLE_INITIAL_SA(1.5));
    with_sag_test(SAG(1.5), TABLE_INITIAL_SA(1));
}

SEASTAR_TEST_CASE(basic_garbage_collection_test) {
    return test_env::do_with_async([] (test_env& env) {
        auto tmp = tmpdir();
        auto s = make_shared_schema({}, "ks", "cf",
                                    {{"p1", utf8_type}}, {{"c1", utf8_type}}, {{"r1", utf8_type}}, {}, utf8_type);

        static constexpr float expired = 0.33;
        // we want number of expired keys to be ~ 1.5*sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE so as to
        // test ability of histogram to return a good estimation after merging keys.
        static int total_keys = std::ceil(sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE/expired)*1.5;

        auto make_insert = [&] (bytes k, uint32_t ttl, uint32_t expiration_time) {
            auto key = partition_key::from_exploded(*s, {k});
            mutation m(s, key);
            auto c_key = clustering_key::from_exploded(*s, {to_bytes("c1")});
            auto live_cell = atomic_cell::make_live(*utf8_type, 0, bytes("a"), gc_clock::time_point(gc_clock::duration(expiration_time)), gc_clock::duration(ttl));
            m.set_clustered_cell(c_key, *s->get_column_definition("r1"), std::move(live_cell));
            return m;
        };
        std::vector<mutation> mutations;
        mutations.reserve(total_keys);

        auto expired_keys = total_keys*expired;
        auto now = gc_clock::now();
        for (auto i = 0; i < expired_keys; i++) {
            // generate expiration time at different time points or only a few entries would be created in histogram
            auto expiration_time = (now - gc_clock::duration(DEFAULT_GC_GRACE_SECONDS*2+i)).time_since_epoch().count();
            mutations.push_back(make_insert(to_bytes("expired_key" + to_sstring(i)), 1, expiration_time));
        }
        auto remaining = total_keys-expired_keys;
        auto expiration_time = (now + gc_clock::duration(3600)).time_since_epoch().count();
        for (auto i = 0; i < remaining; i++) {
            mutations.push_back(make_insert(to_bytes("key" + to_sstring(i)), 3600, expiration_time));
        }

        column_family_for_tests cf(env.manager(), s);
        auto table_s = make_table_state_for_test(env, *cf);
        auto close_cf = deferred_stop(cf);

        auto creator = [&, gen = make_lw_shared<unsigned>(1)] {
            auto sst = env.make_sstable(s, tmp.path().string(), (*gen)++, sstables::get_highest_sstable_version(), big);
            return sst;
        };
        auto sst = make_sstable_containing(creator, std::move(mutations));
        column_family_test(cf).add_sstable(sst);

        const auto& stats = sst->get_stats_metadata();
        BOOST_REQUIRE(stats.estimated_tombstone_drop_time.bin.size() == sstables::TOMBSTONE_HISTOGRAM_BIN_SIZE);
        auto gc_before = gc_clock::now() - s->gc_grace_seconds();
        // Asserts that two keys are equal to within a positive delta
        sstable_run run;
        run.insert(sst);
        BOOST_REQUIRE(std::fabs(run.estimate_droppable_tombstone_ratio(gc_before) - expired) <= 0.1);

        auto cd = sstables::compaction_descriptor({ sst }, default_priority_class());
        cd.enable_garbage_collection(cf->get_sstable_set());
        auto info = compact_sstables(cf.get_compaction_manager(), std::move(cd), *cf, creator).get0();
        auto uncompacted_size = sst->data_size();
        BOOST_REQUIRE(info.new_sstables.size() == 1);
        BOOST_REQUIRE(info.new_sstables.front()->estimate_droppable_tombstone_ratio(gc_before) == 0.0f);
        BOOST_REQUIRE_CLOSE(info.new_sstables.front()->data_size(), uncompacted_size*(1-expired), 5);
        auto control = make_strategy_control_for_test(false);

        // sstable satisfying conditions will be included
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.3f");
            // that's needed because sstable with droppable data should be old enough.
            options.emplace("tombstone_compaction_interval", "0");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            auto descriptor = cs.get_sstables_for_compaction(*table_s, *control, {sst});
            BOOST_REQUIRE(descriptor.sstables.size() == 1);
            BOOST_REQUIRE(descriptor.sstables.front() == sst);
        }

        // sstable with droppable ratio of 0.3 won't be included due to threshold
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_threshold", "0.5f");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            auto descriptor = cs.get_sstables_for_compaction(*table_s, *control, { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
        // sstable which was recently created won't be included due to min interval
        {
            std::map<sstring, sstring> options;
            options.emplace("tombstone_compaction_interval", "3600");
            auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, options);
            sstables::test(sst).set_data_file_write_time(db_clock::now());
            auto descriptor = cs.get_sstables_for_compaction(*table_s, *control, { sst });
            BOOST_REQUIRE(descriptor.sstables.size() == 0);
        }
    });
}

SEASTAR_TEST_CASE(ics_reshape_test) {
    static constexpr unsigned disjoint_sstable_count = 256;

    return test_env::do_with_async([] (test_env& env) {
        auto builder = schema_builder("tests", "ics_reshape_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("cl", ::timestamp_type, column_kind::clustering_key)
                .with_column("value", int32_type);
        builder.set_compaction_strategy(sstables::compaction_strategy_type::incremental);
        constexpr unsigned target_sstable_size_in_mb = 1000;
        std::map <sstring, sstring> opts = {
                {"sstable_size_in_mb", to_sstring(target_sstable_size_in_mb)},
        };
        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::incremental, opts);
        builder.set_compaction_strategy_options(std::move(opts));
        auto s = builder.build();

        auto tokens = token_generation_for_shard(disjoint_sstable_count, this_shard_id(), test_db_config.murmur3_partitioner_ignore_msb_bits(), smp::count);

        auto make_row = [&](unsigned token_idx) {
            auto key_str = tokens[token_idx].first;
            auto key = partition_key::from_exploded(*s, {to_bytes(key_str)});

            mutation m(s, key);
            auto value = 1;
            auto next_ts = 1;
            auto c_key = clustering_key::from_exploded(*s, {::timestamp_type->decompose(next_ts)});
            m.set_clustered_cell(c_key, bytes("value"), data_value(int32_t(value)), next_ts);
            return m;
        };

        auto tmp = tmpdir();

        auto sst_gen = [&env, s, &tmp, gen = make_lw_shared<unsigned>(1)]() {
            return env.make_sstable(s, tmp.path().string(), (*gen)++, sstables::sstable::version_types::md, big);
        };

        {
            auto sstable_count = s->max_compaction_threshold() * 2;

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(sstable_count);
            for (unsigned i = 0; i < sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0)});
                sstables.push_back(std::move(sst));
            }

            auto ret = cs.get_reshaping_job(sstables, s, default_priority_class(), reshape_mode::strict);
            BOOST_REQUIRE(ret.sstables.size() == s->max_compaction_threshold());
            BOOST_REQUIRE(ret.max_sstable_bytes == target_sstable_size_in_mb*1024*1024);
        }

        {
            // create set of 256 disjoint ssts and expect that stcs reshape allows them all to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(i)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(cs.get_reshaping_job(sstables, s, default_priority_class(), reshape_mode::strict).sstables.size() == disjoint_sstable_count);
        }

        {
            // create set of 256 overlapping ssts and expect that stcs reshape allows only 32 to be compacted at once

            std::vector<sstables::shared_sstable> sstables;
            sstables.reserve(disjoint_sstable_count);
            for (unsigned i = 0; i < disjoint_sstable_count; i++) {
                auto sst = make_sstable_containing(sst_gen, {make_row(0)});
                sstables.push_back(std::move(sst));
            }

            BOOST_REQUIRE(cs.get_reshaping_job(sstables, s, default_priority_class(), reshape_mode::strict).sstables.size() == uint64_t(s->max_compaction_threshold()));
        }
    });
}
