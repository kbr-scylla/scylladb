/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#ifndef APPS_SEASTAR_THRIFT_HANDLER_HH_
#define APPS_SEASTAR_THRIFT_HANDLER_HH_

#include "Cassandra.h"
#include "auth/service.hh"
#include "database_fwd.hh"
#include <seastar/core/distributed.hh>
#include "cql3/query_processor.hh"
#include <memory>

struct timeout_config;
class service_permit;
namespace service { class storage_service; }

std::unique_ptr<::cassandra::CassandraCobSvIfFactory> create_handler_factory(distributed<database>& db, distributed<cql3::query_processor>& qp, sharded<service::storage_service>& ss, auth::service&, timeout_config, service_permit& current_permit);

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
