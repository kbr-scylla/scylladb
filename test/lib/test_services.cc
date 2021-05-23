/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "test/lib/test_services.hh"
#include "test/lib/reader_permit.hh"
#include "cdc/generation_service.hh"
#include "db/config.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/view/view_update_generator.hh"
#include "dht/i_partitioner.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "repair/row_level.hh"
#include "service/storage_service.hh"
#include "service/qos/service_level_controller.hh"


class storage_service_for_tests::impl {
    sharded<abort_source> _abort_source;
    sharded<gms::feature_service> _feature_service;
    sharded<gms::gossiper> _gossiper;
    distributed<database> _db;
    db::config _cfg;
    sharded<locator::shared_token_metadata> _token_metadata;
    sharded<service::migration_manager> _migration_manager;
    sharded<db::system_distributed_keyspace> _sys_dist_ks;
    sharded<db::view::view_update_generator> _view_update_generator;
    sharded<netw::messaging_service> _messaging;
    sharded<cdc::generation_service> _cdc_generation_service;
    sharded<repair_service> _repair;
    sharded<auth::service> _auth_service;
    sharded<qos::service_level_controller> _sl_controller;
public:
    impl() {
        auto thread = seastar::thread_impl::get();
        assert(thread);
        _cfg.broadcast_to_all_shards().get();
        utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
        utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));
        _abort_source.start().get();
        _token_metadata.start().get();
        _feature_service.start(gms::feature_config_from_db_config(_cfg)).get();
        _sl_controller.start(std::ref(_auth_service), qos::service_level_options{.shares = 1000}).get();
        _messaging.start(std::ref(_sl_controller), gms::inet_address("127.0.0.1"), 7000).get();
        _gossiper.start(std::ref(_abort_source), std::ref(_feature_service), std::ref(_token_metadata), std::ref(_messaging), std::ref(_cfg)).get();
        service::storage_service_config sscfg;
        sscfg.available_memory = memory::stats().total_memory();
        service::get_storage_service().start(std::ref(_abort_source), std::ref(_db), std::ref(_gossiper), std::ref(_sys_dist_ks), std::ref(_view_update_generator), std::ref(_feature_service), sscfg, std::ref(_migration_manager), std::ref(_token_metadata), std::ref(_messaging), std::ref(_cdc_generation_service), std::ref(_repair), std::ref(_sl_controller), true).get();
        service::get_storage_service().invoke_on_all([] (auto& ss) {
            ss.enable_all_features();
        }).get();
        _cdc_generation_service.start(std::ref(_cfg), std::ref(_gossiper), std::ref(_sys_dist_ks), std::ref(_abort_source), std::ref(_token_metadata)).get();
    }
    ~impl() {
        _cdc_generation_service.stop().get();
        service::get_storage_service().stop().get();
        _messaging.stop().get();
        _db.stop().get();
        _gossiper.stop().get();
        _token_metadata.stop().get();
        _feature_service.stop().get();
        _abort_source.stop().get();
        _sl_controller.stop().get();
    }
};

storage_service_for_tests::storage_service_for_tests() : _impl(std::make_unique<impl>()) {
}

storage_service_for_tests::~storage_service_for_tests() = default;

dht::token create_token_from_key(const dht::i_partitioner& partitioner, sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = partitioner.get_token(key_view);
    assert(token == partitioner.get_token(key_view));
    return token;
}

range<dht::token> create_token_range_from_keys(const dht::sharder& sinfo, const dht::i_partitioner& partitioner, sstring start_key, sstring end_key) {
    dht::token start = create_token_from_key(partitioner, start_key);
    assert(this_shard_id() == sinfo.shard_of(start));
    dht::token end = create_token_from_key(partitioner, end_key);
    assert(this_shard_id() == sinfo.shard_of(end));
    assert(end >= start);
    return range<dht::token>::make(start, end);
}

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

db::nop_large_data_handler nop_lp_handler;
db::config test_db_config;
gms::feature_service test_feature_service(gms::feature_config_from_db_config(test_db_config));

column_family::config column_family_test_config(sstables::sstables_manager& sstables_manager) {
    column_family::config cfg;
    cfg.sstables_manager = &sstables_manager;
    cfg.compaction_concurrency_semaphore = &tests::semaphore();
    return cfg;
}

column_family_for_tests::column_family_for_tests(sstables::sstables_manager& sstables_manager)
    : column_family_for_tests(
        sstables_manager,
        schema_builder(some_keyspace, some_column_family)
            .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
            .build()
    )
{ }

column_family_for_tests::column_family_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s)
    : _data(make_lw_shared<data>())
{
    _data->s = s;
    _data->cfg = column_family_test_config(sstables_manager);
    _data->cfg.enable_disk_writes = false;
    _data->cfg.enable_commitlog = false;
    _data->cf = make_lw_shared<column_family>(_data->s, _data->cfg, column_family::no_commitlog(), _data->cm, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
}
