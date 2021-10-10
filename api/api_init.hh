/*
 * Copyright 2016 ScylaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <seastar/http/httpd.hh>
#include <seastar/core/future.hh>

#include "database_fwd.hh"
#include "seastarx.hh"

namespace service {

class load_meter;
class storage_proxy;
class storage_service;

} // namespace service

namespace locator {

class token_metadata;
class shared_token_metadata;

} // namespace locator

namespace cql_transport { class controller; }
class thrift_controller;
namespace db {
class snapshot_ctl;
class config;
}
namespace netw { class messaging_service; }
class repair_service;
namespace cdc { class generation_service; }

namespace gms {

class gossiper;

}

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    service::load_meter& lmeter;
    const sharded<locator::shared_token_metadata>& shared_token_metadata;

    http_context(distributed<database>& _db,
            distributed<service::storage_proxy>& _sp,
            service::load_meter& _lm, const sharded<locator::shared_token_metadata>& _stm)
            : db(_db), sp(_sp), lmeter(_lm), shared_token_metadata(_stm) {
    }

    const locator::token_metadata& get_token_metadata();
};

future<> set_server_init(http_context& ctx);
future<> set_server_config(http_context& ctx, const db::config& cfg);
future<> set_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx, sharded<service::storage_service>& ss, sharded<gms::gossiper>& g, sharded<cdc::generation_service>& cdc_gs);
future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair);
future<> unset_server_repair(http_context& ctx);
future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl);
future<> unset_transport_controller(http_context& ctx);
future<> set_rpc_controller(http_context& ctx, thrift_controller& ctl);
future<> unset_rpc_controller(http_context& ctx);
future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl);
future<> unset_server_snapshot(http_context& ctx);
future<> set_server_gossip(http_context& ctx, sharded<gms::gossiper>& g);
future<> set_server_load_sstable(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms);
future<> unset_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx, sharded<service::storage_service>& ss);
future<> set_server_stream_manager(http_context& ctx);
future<> set_hinted_handoff(http_context& ctx, sharded<gms::gossiper>& g);
future<> unset_hinted_handoff(http_context& ctx);
future<> set_server_gossip_settle(http_context& ctx, sharded<gms::gossiper>& g);
future<> set_server_cache(http_context& ctx);
future<> set_server_compaction_manager(http_context& ctx);
future<> set_server_done(http_context& ctx);

}
