/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"

namespace cql_transport { class controller; }
class thrift_controller;

namespace api {

void set_storage_service(http_context& ctx, routes& r);
void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, routes& r);
void set_rpc_controller(http_context& ctx, routes& r, thrift_controller& ctl);
void unset_rpc_controller(http_context& ctx, routes& r);
void set_snapshot(http_context& ctx, routes& r);

}
