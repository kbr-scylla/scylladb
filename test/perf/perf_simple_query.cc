/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <json/json.h>

#include <boost/range/irange.hpp>
#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/testing/test_runner.hh>
#include "test/lib/random_utils.hh"
#include "db/config.hh"

#include "schema_builder.hh"
#include "release.hh"
#include <fstream>

static const sstring table_name = "cf";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

static void execute_update_for_key(cql_test_env& env, const bytes& key) {
    env.execute_cql(sprint("UPDATE cf SET "
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
        "WHERE \"KEY\"= 0x%s;", to_hex(key))).get();
};

static void execute_counter_update_for_key(cql_test_env& env, const bytes& key) {
    env.execute_cql(sprint("UPDATE cf SET "
        "\"C0\" = \"C0\" + 1,"
        "\"C1\" = \"C1\" + 2,"
        "\"C2\" = \"C2\" + 3,"
        "\"C3\" = \"C3\" + 4,"
        "\"C4\" = \"C4\" + 5 "
        "WHERE \"KEY\"= 0x%s;", to_hex(key))).get();
};

struct test_config {
    enum class run_mode { read, write, del };

    run_mode mode;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
    bool counters;
    bool flush_memtables;
    unsigned operations_per_shard = 0;
};

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << "}";
}

static void create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        if (cfg.counters) {
            execute_counter_update_for_key(env, make_key(sequence));
        } else {
            execute_update_for_key(env, make_key(sequence));
        }
    }

    if (cfg.flush_memtables) {
        std::cout << "Flushing partitions..." << std::endl;
        env.db().invoke_on_all(&database::flush_all_memtables).get();
    }
}

static bytes make_random_key(test_config& cfg) {
    return make_key(cfg.query_single_key ? 0 : tests::random::get_int<uint64_t>(cfg.partitions - 1));
}

static std::vector<perf_result> test_read(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    auto id = env.prepare("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = ?").get0();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
}

static std::vector<perf_result> test_write(cql_test_env& env, test_config& cfg) {
    auto id = env.prepare("UPDATE cf SET "
                           "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
                           "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
                           "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
                           "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
                           "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
                           "WHERE \"KEY\" = ?;").get0();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
}

static std::vector<perf_result> test_delete(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    auto id = env.prepare("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" FROM cf WHERE \"KEY\" = ?").get0();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
}

static std::vector<perf_result> test_counter_update(cql_test_env& env, test_config& cfg) {
    auto id = env.prepare("UPDATE cf SET "
                           "\"C0\" = \"C0\" + 1,"
                           "\"C1\" = \"C1\" + 2,"
                           "\"C2\" = \"C2\" + 3,"
                           "\"C3\" = \"C3\" + 4,"
                           "\"C4\" = \"C4\" + 5 "
                           "WHERE \"KEY\" = ?;").get0();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
}

static schema_ptr make_counter_schema(std::string_view ks_name) {
    return schema_builder(ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key)
            .with_column("C0", counter_type)
            .with_column("C1", counter_type)
            .with_column("C2", counter_type)
            .with_column("C3", counter_type)
            .with_column("C4", counter_type)
            .build();
}

static std::vector<perf_result> do_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name);
        }
        return schema({}, ks_name, "cf",
                {{"KEY", bytes_type}},
                {},
                {{"C0", bytes_type}, {"C1", bytes_type}, {"C2", bytes_type}, {"C3", bytes_type}, {"C4", bytes_type}},
                {},
                utf8_type);
    }).get();

    switch (cfg.mode) {
    case test_config::run_mode::read:
        return test_read(env, cfg);
    case test_config::run_mode::write:
        if (cfg.counters) {
            return test_counter_update(env, cfg);
        } else {
            return test_write(env, cfg);
        }
    case test_config::run_mode::del:
        return test_delete(env, cfg);
    };
    abort();
}

void write_json_result(std::string result_file, const test_config& cfg, perf_result median, double mad, double max, double min) {
    Json::Value results;

    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["partitions"] = cfg.partitions;
    params["cpus"] = smp::count;
    params["duration"] = cfg.duration_in_seconds;
    params["concurrency,partitions,cpus,duration"] = fmt::format("{},{},{},{}", cfg.concurrency, cfg.partitions, smp::count, cfg.duration_in_seconds);
    results["parameters"] = std::move(params);

    Json::Value stats;
    stats["median tps"] = median.throughput;
    stats["allocs_per_op"] = median.mallocs_per_op;
    stats["tasks_per_op"] = median.tasks_per_op;
    stats["mad tps"] = mad;
    stats["max tps"] = max;
    stats["min tps"] = min;
    results["stats"] = std::move(stats);

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }
    results["test_properties"]["type"] = test_type;

    // <version>-<release>
    auto version_components = std::vector<std::string>{};
    auto sver = scylla_version();
    boost::algorithm::split(version_components, sver, boost::is_any_of("-"));
    // <scylla-build>.<date>.<git-hash>
    auto release_components = std::vector<std::string>{};
    boost::algorithm::split(release_components, version_components[1], boost::is_any_of("."));

    Json::Value version;
    version["commit_id"] = release_components[2];
    version["date"] = release_components[1];
    version["version"] = version_components[0];

    // It'd be nice to have std::chrono::format(), wouldn't it?
    auto current_time = std::time(nullptr);
    char time_str[100];
    ::tm time_buf;
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", ::localtime_r(&current_time, &time_buf));
    version["run_date_time"] = time_str;

    results["versions"]["scylla-server"] = std::move(version);

    auto out = std::ofstream(result_file);
    out << results;
}

/// If app configuration contains the named parameter, store its value into \p store.
static void set_from_cli(const char* name, app_template& app, utils::config_file::named_value<sstring>& store) {
    const auto& cfg = app.configuration();
    auto found = cfg.find(name);
    if (found != cfg.end()) {
        store(found->second.as<std::string>());
    }
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("delete", "test delete path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test reading with a single key instead of random keys")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("counters", "test counters")
        ("flush", "flush memtables before test")
        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ("audit", bpo::value<std::string>(), "value for audit config entry")
        ("audit-keyspaces", bpo::value<std::string>(), "value for audit_keyspaces config entry")
        ("audit-tables", bpo::value<std::string>(), "value for audit_tables config entry")
        ("audit-categories", bpo::value<std::string>(), "value for audit_categories config entry")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';
        return smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        }).then([&app] {
            cql_test_config testcfg;
            testcfg.db_config = ::make_shared<db::config>();
            set_from_cli("audit", app, testcfg.db_config->audit);
            set_from_cli("audit-keyspaces", app, testcfg.db_config->audit_keyspaces);
            set_from_cli("audit-tables", app, testcfg.db_config->audit_tables);
            set_from_cli("audit-categories", app, testcfg.db_config->audit_categories);
          return do_with_cql_env_thread([&app] (auto&& env) {
            auto cfg = test_config();
            cfg.partitions = app.configuration()["partitions"].as<unsigned>();
            cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg.query_single_key = app.configuration().contains("query-single-key");
            cfg.counters = app.configuration().contains("counters");
            cfg.flush_memtables = app.configuration().contains("flush");
            if (app.configuration().contains("write")) {
                cfg.mode = test_config::run_mode::write;
            } else if (app.configuration().contains("delete")) {
                cfg.mode = test_config::run_mode::del;
            } else {
                cfg.mode = test_config::run_mode::read;
            };
            if (app.configuration().contains("operations-per-shard")) {
                cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            audit::audit::create_audit(env.local_db().get_config()).handle_exception([&] (auto&& e) {
                fmt::print("audit creation failed: {}", e);
            }).get();
            audit::audit::start_audit(env.local_db().get_config(), env.qp()).get();
            auto audit_stop = defer([] {
                audit::audit::stop_audit().get();
            });
            auto results = do_test(env, cfg);

            auto compare_throughput = [] (perf_result a, perf_result b) { return a.throughput < b.throughput; };
            std::sort(results.begin(), results.end(), compare_throughput);
            auto median_result = results[results.size() / 2];
            auto median = median_result.throughput;
            auto min = results[0].throughput;
            auto max = results[results.size() - 1].throughput;
            auto absolute_deviations = boost::copy_range<std::vector<double>>(
                    results
                    | boost::adaptors::transformed(std::mem_fn(&perf_result::throughput))
                    | boost::adaptors::transformed([&] (double r) { return abs(r - median); }));
            std::sort(absolute_deviations.begin(), absolute_deviations.end());
            auto mad = absolute_deviations[results.size() / 2];
            std::cout << format("\nmedian {}\nmedian absolute deviation: {:.2f}\nmaximum: {:.2f}\nminimum: {:.2f}\n", median_result, mad, max, min);

            if (app.configuration().contains("json-result")) {
                write_json_result(app.configuration()["json-result"].as<std::string>(), cfg, median_result, mad, max, min);
            }
          }, testcfg);
        });
    });
}
