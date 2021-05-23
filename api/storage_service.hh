/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"
#include "db/data_listeners.hh"

namespace cql_transport { class controller; }
class thrift_controller;
namespace db { class snapshot_ctl; }
namespace netw { class messaging_service; }
class repair_service;

namespace api {

void set_storage_service(http_context& ctx, routes& r);
void set_repair(http_context& ctx, routes& r, sharded<repair_service>& repair);
void unset_repair(http_context& ctx, routes& r);
void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, routes& r);
void set_rpc_controller(http_context& ctx, routes& r, thrift_controller& ctl);
void unset_rpc_controller(http_context& ctx, routes& r);
void set_snapshot(http_context& ctx, routes& r, sharded<db::snapshot_ctl>& snap_ctl);
void unset_snapshot(http_context& ctx, routes& r);
seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request = false);

}
