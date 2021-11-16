/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "timeout_config.hh"
#include "redis/service.hh"
#include "redis/keyspace_utils.hh"
#include "redis/server.hh"
#include "service/storage_proxy.hh"
#include "db/config.hh"
#include "log.hh"
#include "auth/common.hh"
#include "database.hh"

static logging::logger slogger("redis_service");

redis_service::redis_service(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<auth::service>& auth_service,
        seastar::sharded<service::migration_manager>& mm, db::config& cfg, seastar::sharded<gms::gossiper>& gossiper)
    : _proxy(proxy)
    , _auth_service(auth_service)
    , _mm(mm)
    , _cfg(cfg)
    , _gossiper(gossiper)
{
}

redis_service::~redis_service()
{
}

future<> redis_service::listen(seastar::sharded<auth::service>& auth_service, db::config& cfg)
{
    if (_server) {
        return make_ready_future<>();
    }
    auto server = make_shared<seastar::sharded<redis_transport::redis_server>>();
    _server = server;

    auto preferred = cfg.rpc_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = cfg.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);
    auto ceo = cfg.client_encryption_options();
    auto keepalive = cfg.rpc_keepalive();
    redis_transport::redis_server_config redis_cfg;
    redis_cfg._timeout_config = make_timeout_config(cfg);
    redis_cfg._read_consistency_level = make_consistency_level(cfg.redis_read_consistency_level());
    redis_cfg._write_consistency_level = make_consistency_level(cfg.redis_write_consistency_level());
    redis_cfg._max_request_size = memory::stats().total_memory() / 10;
    redis_cfg._total_redis_db_count = cfg.redis_database_count();
    return utils::resolve(cfg.rpc_address, family, preferred).then([this, server, &cfg, keepalive, ceo = std::move(ceo), redis_cfg, &auth_service] (seastar::net::inet_address ip) {
        return server->start(std::ref(_query_processor), std::ref(auth_service), redis_cfg).then([this, server, &cfg, ip, ceo, keepalive]() {
            auto f = make_ready_future();
            struct listen_cfg {
                socket_address addr;
                std::shared_ptr<seastar::tls::credentials_builder> cred;
            };

            _listen_addresses.clear();
            std::vector<listen_cfg> configs;
            if (cfg.redis_port()) {
                configs.emplace_back(listen_cfg { {socket_address{ip, cfg.redis_port()}} });
                _listen_addresses.push_back(configs.back().addr);
            }

            // main should have made sure values are clean and neatish
            if (utils::is_true(utils::get_or_default(ceo, "enabled", "false"))) {
                auto cred = std::make_shared<seastar::tls::credentials_builder>();
                f = utils::configure_tls_creds_builder(*cred, std::move(ceo));

                slogger.info("Enabling encrypted REDIS connections between client and server");

                if (cfg.redis_ssl_port() && cfg.redis_ssl_port() != cfg.redis_port()) {
                    configs.emplace_back(listen_cfg{{ip, cfg.redis_ssl_port()}, std::move(cred)});
                    _listen_addresses.push_back(configs.back().addr);
                } else {
                    configs.back().cred = std::move(cred);
                }
            }

            return f.then([server, configs = std::move(configs), keepalive] {
                return parallel_for_each(configs, [server, keepalive](const listen_cfg & cfg) {
                    return server->invoke_on_all(&redis_transport::redis_server::listen, cfg.addr, cfg.cred, false, keepalive).then([cfg] {
                        slogger.info("Starting listening for REDIS clients on {} ({})", cfg.addr, cfg.cred ? "encrypted" : "unencrypted");
                    });
                });
            });
        });
    }).handle_exception([this](auto ep) {
        return _server->stop().then([ep = std::move(ep)]() mutable {
            return make_exception_future<>(std::move(ep));
        });
    });
}

sstring redis_service::name() const {
    return "redis";
}

sstring redis_service::protocol() const {
    return "RESP";
}

sstring redis_service::protocol_version() const {
    return ::redis::version;
}

std::vector<socket_address> redis_service::listen_addresses() const {
    return _listen_addresses;
}

bool redis_service::is_server_running() const {
    return !_listen_addresses.empty();
}

future<> redis_service::start_server()
{
    // 1. Create keyspace/tables used by redis API if not exists.
    // 2. Initialize the redis query processor.
    // 3. Listen on the redis transport port.
    return redis::maybe_create_keyspace(_mm, _cfg, _gossiper).then([this] {
        return _query_processor.start(std::ref(_proxy), std::ref(_proxy.local().get_db()));
    }).then([this] {
        return _query_processor.invoke_on_all([] (auto& processor) {
            return processor.start();
        });
    }).then([this] {
        return listen(_auth_service, _cfg);
    });
}

future<> redis_service::stop_server()
{
    // If the redis protocol disable, the redis_service::init is not
    // invoked at all. Do nothing if `_server is null.
    if (_server) {
        return _server->stop().then([this] {
            _listen_addresses.clear();
            return _query_processor.stop();
        });
    }
    return make_ready_future<>();
}

future<> redis_service::request_stop_server() {
    return stop_server();
}
