/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>

using namespace seastar;

namespace cql_transport { class cql_server; }
namespace auth { class service; }
namespace service {
    class migration_notifier;
    class memory_limiter;
}
namespace gms { class gossiper; }
namespace cql3 { class query_processor; }
namespace qos { class service_level_controller; }
namespace db { class config; }

namespace cql_transport {

class controller {
    std::unique_ptr<distributed<cql_transport::cql_server>> _server;
    semaphore _ops_sem; /* protects start/stop operations on _server */
    bool _stopped = false;

    sharded<auth::service>& _auth_service;
    sharded<service::migration_notifier>& _mnotifier;
    gms::gossiper& _gossiper;
    sharded<cql3::query_processor>& _qp;
    sharded<service::memory_limiter>& _mem_limiter;
    sharded<qos::service_level_controller>& _sl_controller;
    const db::config& _config;

    future<> set_cql_ready(bool ready);
    future<> do_start_server();
    future<> do_stop_server();

public:
    controller(sharded<auth::service>&, sharded<service::migration_notifier>&, gms::gossiper&,
            sharded<cql3::query_processor>&, sharded<service::memory_limiter>&,
            sharded<qos::service_level_controller>&, const db::config& cfg);
    future<> start_server();
    future<> stop_server();
    future<> stop();
    future<bool> is_server_running();
};

} // namespace cql_transport
