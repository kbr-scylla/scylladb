/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <seastar/core/coroutine.hh>
#include "redis/keyspace_utils.hh"
#include "schema_builder.hh"
#include "types.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "seastar/core/future.hh"
#include <memory>
#include "log.hh"
#include "db/query_context.hh"
#include "auth/service.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "transport/server.hh"
#include "db/system_keyspace.hh"
#include "schema.hh"
#include "replica/database.hh"
#include "gms/gossiper.hh"
#include <seastar/core/print.hh>
#include "db/config.hh"

using namespace seastar;

namespace redis {

static logging::logger logger("keyspace_utils");
schema_ptr strings_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::STRINGs), ks_name, redis::STRINGs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save STRINGs for redis"
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr lists_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::LISTs), ks_name, redis::LISTs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", bytes_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save LISTs for redis"
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr hashes_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::HASHes), ks_name, redis::HASHes,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save HASHes for redis"
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr sets_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::SETs), ks_name, redis::SETs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save SETs for redis"
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr zsets_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::ZSETs), ks_name, redis::ZSETs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", double_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save ZSETs for redis"
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

future<> create_keyspace_if_not_exists_impl(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<service::migration_manager>& mm, db::config& config, int default_replication_factor) {
    assert(this_shard_id() == 0);
    auto keyspace_replication_strategy_options = config.redis_keyspace_replication_strategy_options();
    if (!keyspace_replication_strategy_options.contains("class")) {
        keyspace_replication_strategy_options["class"] = "SimpleStrategy";
        keyspace_replication_strategy_options["replication_factor"] = fmt::format("{}", default_replication_factor);
    }
    auto keyspace_gen = [&proxy, &mm, &config, keyspace_replication_strategy_options = std::move(keyspace_replication_strategy_options)]  (sstring name) -> future<> {
        auto& mml = mm.local();
        auto& proxyl = proxy.local();
        if (proxyl.get_db().local().has_keyspace(name)) {
            co_return;
        }
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
        attrs->add_property(cql3::statements::ks_prop_defs::KW_DURABLE_WRITES, "true");
        std::map<sstring, sstring> replication_properties;
        for (auto&& option : keyspace_replication_strategy_options) {
            replication_properties.emplace(option.first, option.second);
        }
        attrs->add_property(cql3::statements::ks_prop_defs::KW_REPLICATION, replication_properties); 
        attrs->validate();
        const auto& tm = *proxyl.get_token_metadata_ptr();
        co_return co_await mml.announce(mml.prepare_new_keyspace_announcement(attrs->as_ks_metadata(name, tm)));
    };
    auto table_gen = [&proxy, &mm] (sstring ks_name, sstring cf_name, schema_ptr schema) -> future<> {
        auto& mml= mm.local();
        auto& proxyl = proxy.local();
        if (proxyl.get_db().local().has_schema(ks_name, cf_name)) {
            co_return;
        }
        logger.info("Create keyspace: {}, table: {} for redis.", ks_name, cf_name);
        co_return co_await mml.announce(co_await mml.prepare_new_column_family_announcement(schema));
    };

    co_await mm.local().schema_read_barrier();

    // create default databases for redis.
    co_return co_await parallel_for_each(boost::irange<unsigned>(0, config.redis_database_count()), [keyspace_gen = std::move(keyspace_gen), table_gen = std::move(table_gen)] (auto c) {
        auto ks_name = fmt::format("REDIS_{}", c);
        return keyspace_gen(ks_name).then([ks_name, table_gen] {
            return when_all_succeed(
                table_gen(ks_name, redis::STRINGs, strings_schema(ks_name)),
                table_gen(ks_name, redis::LISTs, lists_schema(ks_name)),
                table_gen(ks_name, redis::SETs, sets_schema(ks_name)),
                table_gen(ks_name, redis::HASHes, hashes_schema(ks_name)),
                table_gen(ks_name, redis::ZSETs, zsets_schema(ks_name))
            ).discard_result();
        });
    });
}

future<> maybe_create_keyspace(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<service::migration_manager>& mm, db::config& config, sharded<gms::gossiper>& gossiper) {
    return gms::get_up_endpoint_count(gossiper.local()).then([&proxy, &mm, &config] (auto live_endpoint_count) {
        int replication_factor = 3;
        if (live_endpoint_count < replication_factor) {
            replication_factor = 1;
            logger.warn("Creating keyspace for redis with unsafe, live endpoint nodes count: {}.", live_endpoint_count);
        }
        return create_keyspace_if_not_exists_impl(proxy, mm, config, replication_factor);
    });
}

}
