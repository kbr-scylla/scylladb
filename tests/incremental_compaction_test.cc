/*
 * Copyright (C) 2019 ScyllaDB
 *
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/test/unit_test.hpp>
#include <memory>
#include <utility>

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/distributed.hh>
#include <seastar/testing/test_case.hh>
#include "sstables/sstables.hh"
#include "schema.hh"
#include "database.hh"
#include "sstables/compaction_manager.hh"
#include "mutation_reader.hh"
#include "sstable_test.hh"
#include "tmpdir.hh"
#include "cell_locking.hh"
#include "flat_mutation_reader_assertions.hh"
#include "service/storage_proxy.hh"
#include "sstable_run_based_compaction_strategy_for_tests.hh"
#include "dht/i_partitioner.hh"
#include "dht/murmur3_partitioner.hh"
#include "db/large_data_handler.hh"

#include "sstable_utils.hh"

using namespace sstables;

static flat_mutation_reader sstable_reader(shared_sstable sst, schema_ptr s) {
    return sst->as_mutation_source().make_reader(s, query::full_partition_range, s->full_slice());

}

namespace dht {
    extern std::unique_ptr<i_partitioner> default_partitioner;
}

static std::vector<std::pair<sstring, dht::token>> token_generation_for_shard(unsigned tokens_to_generate, unsigned shard,
        unsigned ignore_msb = 0, unsigned smp_count = smp::count) {
    unsigned tokens = 0;
    unsigned key_id = 0;
    std::vector<std::pair<sstring, dht::token>> key_and_token_pair;

    key_and_token_pair.reserve(tokens_to_generate);
    dht::default_partitioner = std::make_unique<dht::murmur3_partitioner>(smp_count, ignore_msb);

    while (tokens < tokens_to_generate) {
        sstring key = to_sstring(key_id++);
        dht::token token = create_token_from_key(key);
        if (shard != dht::global_partitioner().shard_of(token)) {
            continue;
        }
        tokens++;
        key_and_token_pair.emplace_back(key, token);
    }
    assert(key_and_token_pair.size() == tokens_to_generate);

    std::sort(key_and_token_pair.begin(),key_and_token_pair.end(), [] (auto& i, auto& j) {
        return i.second < j.second;
    });

    return key_and_token_pair;
}

static std::vector<std::pair<sstring, dht::token>> token_generation_for_current_shard(unsigned tokens_to_generate) {
    return token_generation_for_shard(tokens_to_generate, engine().cpu_id());
}

SEASTAR_TEST_CASE(incremental_compaction_test) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        cell_locker_stats cl_stats;

        auto builder = schema_builder("tests", "incremental_compaction_test")
                .with_column("id", utf8_type, column_kind::partition_key)
                .with_column("value", int32_type);
        auto s = builder.build();

        auto tmp = make_lw_shared<tmpdir>();
        auto sst_gen = [s, tmp, gen = make_lw_shared<unsigned>(1)] () mutable {
            auto sst = make_sstable(s, tmp->path().string(), (*gen)++, la, big);
            sst->set_unshared();
            return sst;
        };

        auto cm = make_lw_shared<compaction_manager>();
        auto tracker = make_lw_shared<cache_tracker>();
        auto cf = make_lw_shared<column_family>(s, column_family_test_config(), column_family::no_commitlog(), *cm, cl_stats, *tracker);
        cf->mark_ready_for_writes();
        cf->start();
        cf->set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
        auto compact = [&, s] (std::vector<shared_sstable> all, auto replacer) -> std::vector<shared_sstable> {
            return sstables::compact_sstables(sstables::compaction_descriptor(std::move(all), 1, 0), *cf, sst_gen, replacer).get0().new_sstables;
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
        std::optional<utils::observer<sstable&>> observer;
        sstables::sstable_run_based_compaction_strategy_for_tests cs;

        auto do_replace = [&] (auto old_sstables, auto new_sstables, auto& expected_sst, auto& closed_sstables_tracker) {
            // that's because each sstable will contain only 1 mutation.
            BOOST_REQUIRE(old_sstables.size() == 1);
            BOOST_REQUIRE(new_sstables.size() == 1);
            // check that sstable replacement follows token order
            BOOST_REQUIRE(*expected_sst == old_sstables.front()->generation());
            expected_sst++;
            // check that previously released sstable was already closed
            BOOST_REQUIRE(*closed_sstables_tracker == old_sstables.front()->generation());

            BOOST_REQUIRE(sstables.count(old_sstables.front()));
            BOOST_REQUIRE(!sstables.count(new_sstables.front()));
            sstables.erase(old_sstables.front());
            sstables.insert(new_sstables.front());
            column_family_test(cf).rebuild_sstable_list(new_sstables, old_sstables);
            cf->get_compaction_manager().propagate_replacement(&*cf, old_sstables, new_sstables);
            observer = old_sstables.front()->add_on_closed_handler([&] (sstable& sst) {
                BOOST_TEST_MESSAGE(sprint("Closing sstable of generation %d", sst.generation()));
                closed_sstables_tracker++;
            });

            BOOST_TEST_MESSAGE(sprint("Removing sstable of generation %d, refcnt: %d", old_sstables.front()->generation(), old_sstables.front().use_count()));
        };

        auto do_compaction = [&] (size_t expected_input, size_t expected_output) -> std::vector<shared_sstable> {
            auto input_ssts = std::vector<shared_sstable>(sstables.begin(), sstables.end());
            auto desc = cs.get_sstables_for_compaction(*cf, std::move(input_ssts));

            // nothing to compact, move on.
            if (desc.sstables.empty()) {
                return {};
            }

            BOOST_REQUIRE(desc.sstables.size() == expected_input);
            auto sstable_run = boost::copy_range<std::set<int64_t>>(desc.sstables
                | boost::adaptors::transformed([] (auto& sst) { return sst->generation(); }));
            auto expected_sst = sstable_run.begin();
            auto closed_sstables_tracker = sstable_run.begin();
            auto replacer = [&] (auto old_sstables, auto new_sstables) {
                BOOST_REQUIRE(expected_sst != sstable_run.end());
                do_replace(std::move(old_sstables), std::move(new_sstables), expected_sst, closed_sstables_tracker);
            };

            auto result = compact(std::move(desc.sstables), replacer);
            observer->disconnect();
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
            assert_that(sstable_reader(result[i], s))
                .produces(make_insert(tokens[i]))
                .produces_end_of_stream();
        }
    });
}

