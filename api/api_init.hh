/*
 * Copyright 2016 ScylaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once
#include "database.hh"
#include "service/storage_proxy.hh"
#include "http/httpd.hh"

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    http_context(distributed<database>& _db,
            distributed<service::storage_proxy>& _sp)
            : db(_db), sp(_sp) {
    }
};

future<> set_server_init(http_context& ctx);
future<> set_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx);
future<> set_server_gossip(http_context& ctx);
future<> set_server_load_sstable(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx);
future<> set_server_stream_manager(http_context& ctx);
future<> set_server_gossip_settle(http_context& ctx);
future<> set_server_done(http_context& ctx);


}
