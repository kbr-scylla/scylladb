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

redis_service::redis_service()
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

    auto addr = cfg.rpc_address();
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
    return gms::inet_address::lookup(addr, family, preferred).then([this, server, addr, &cfg, keepalive, ceo = std::move(ceo), redis_cfg, &auth_service] (seastar::net::inet_address ip) {
        return server->start(std::ref(_query_processor), std::ref(auth_service), redis_cfg).then([server, &cfg, addr, ip, ceo, keepalive]() {
            auto f = make_ready_future();
            struct listen_cfg {
                socket_address addr;
                std::shared_ptr<seastar::tls::credentials_builder> cred;
            };

            std::vector<listen_cfg> configs;
            if (cfg.redis_port()) {
                configs.emplace_back(listen_cfg { {socket_address{ip, cfg.redis_port()}} });
            }

            // main should have made sure values are clean and neatish
            if (utils::is_true(utils::get_or_default(ceo, "enabled", "false"))) {
                auto cred = std::make_shared<seastar::tls::credentials_builder>();
                f = utils::configure_tls_creds_builder(*cred, std::move(ceo));

                slogger.info("Enabling encrypted REDIS connections between client and server");

                if (cfg.redis_ssl_port() && cfg.redis_ssl_port() != cfg.redis_port()) {
                    configs.emplace_back(listen_cfg{{ip, cfg.redis_ssl_port()}, std::move(cred)});
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

future<> redis_service::init(seastar::sharded<service::storage_proxy>& proxy, seastar::sharded<database>& db,
        seastar::sharded<auth::service>& auth_service, seastar::sharded<service::migration_manager>& mm, db::config& cfg)
{
    // 1. Create keyspace/tables used by redis API if not exists.
    // 2. Initialize the redis query processor.
    // 3. Listen on the redis transport port.
    return redis::maybe_create_keyspace(mm, cfg).then([this, &proxy, &db] {
        return _query_processor.start(std::ref(proxy), std::ref(db));
    }).then([this] {
        return _query_processor.invoke_on_all([] (auto& processor) {
            return processor.start();
        });
    }).then([this, &cfg, &auth_service] {
        return listen(auth_service, cfg);
    });
}

future<> redis_service::stop()
{
    // If the redis protocol disable, the redis_service::init is not
    // invoked at all. Do nothing if `_server is null.
    if (_server) {
        return _server->stop().then([this] {
            return _query_processor.stop();
        });
    }
    return make_ready_future<>();
}
