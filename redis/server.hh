/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "redis/options.hh"
#include "redis/protocol_parser.hh"
#include "redis/query_processor.hh"
#include "redis/reply.hh"
#include "redis/request.hh"
#include "redis/stats.hh"

#include "auth/authenticator.hh"
#include "auth/service.hh"
#include "cql3/values.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "timeout_config.hh"
#include "utils/estimated_histogram.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "generic_server.hh"

#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/net/tls.hh>

#include <memory>

db::consistency_level make_consistency_level(const sstring&);

class redis_exception;

namespace service {

class storage_proxy;

}

namespace redis_transport {

struct redis_server_config {
    ::timeout_config _timeout_config;
    size_t _max_request_size;
    db::consistency_level _read_consistency_level;
    db::consistency_level _write_consistency_level;
    size_t _total_redis_db_count;
};

class redis_server : public generic_server::server {
    seastar::sharded<redis::query_processor>& _query_processor;
    redis_server_config _config;
    size_t _max_request_size;
    semaphore _memory_available;
    redis::stats _stats;
    auth::service& _auth_service;
    size_t _total_redis_db_count;

public:
    redis_server(seastar::sharded<redis::query_processor>& qp, auth::service& auth_service, redis_server_config config);

    struct result {
        result(redis::redis_message&& m) : _data(make_foreign(std::make_unique<redis::redis_message>(std::move(m)))) {}
        foreign_ptr<std::unique_ptr<redis::redis_message>> _data;
        inline lw_shared_ptr<scattered_message<char>> make_message()  {
            return _data->message();
        }
    };
    using response_type = result;

private:
    class connection : public generic_server::connection {
        redis_server& _server;
        socket_address _server_addr;
        redis_protocol_parser _parser;
        redis::redis_options _options;

        using execution_stage_type = inheriting_concrete_execution_stage<
                future<redis_server::result>,
                redis_server::connection*,
                redis::request&&,
                redis::redis_options&,
                service_permit
        >;
        static thread_local execution_stage_type _process_request_stage;
    public:
        connection(redis_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr);
        virtual ~connection();
        future<> process_request() override;
        void handle_error(future<>&& f) override;
        void write_reply(const redis_exception&);
        void write_reply(redis_server::result result);
    private:
        const ::timeout_config& timeout_config() { return _server.timeout_config(); }
        future<result> process_request_one(redis::request&& request, redis::redis_options&, service_permit permit);
        future<result> process_request_internal();
    };

    virtual shared_ptr<generic_server::connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) override;
    future<> unadvertise_connection(shared_ptr<generic_server::connection> conn) override;

    const ::timeout_config& timeout_config() { return _config._timeout_config; }
};
}
