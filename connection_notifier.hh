/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */
#pragma once

#include "db/query_context.hh"

#include <seastar/net/inet_address.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include <optional>

namespace db {
extern const char *const system_keyspace_CLIENTS;
}

enum class client_type {
    cql = 0,
    thrift,
    alternator,
};

sstring to_string(client_type ct);

enum class changed_column {
    username = 0,
    connection_stage,
    driver_name,
    driver_version,
    hostname,
    protocol_version,
};

template <changed_column column> constexpr const char* column_literal = "";
template <> inline constexpr const char* column_literal<changed_column::username> = "username";
template <> inline constexpr const char* column_literal<changed_column::connection_stage> = "connection_stage";
template <> inline constexpr const char* column_literal<changed_column::driver_name> = "driver_name";
template <> inline constexpr const char* column_literal<changed_column::driver_version> = "driver_version";
template <> inline constexpr const char* column_literal<changed_column::hostname> = "hostname";
template <> inline constexpr const char* column_literal<changed_column::protocol_version> = "protocol_version";

enum class client_connection_stage {
    established = 0,
    authenticating,
    ready,
};

template <client_connection_stage ccs> constexpr const char* connection_stage_literal = "";
template <> inline constexpr const char* connection_stage_literal<client_connection_stage::established> = "ESTABLISHED";
template <> inline constexpr const char* connection_stage_literal<client_connection_stage::authenticating> = "AUTHENTICATING";
template <> inline constexpr const char* connection_stage_literal<client_connection_stage::ready> = "READY";

// Representation of a row in `system.clients'. std::optionals are for nullable cells.
struct client_data {
    net::inet_address ip;
    int32_t port;
    client_type ct;
    client_connection_stage connection_stage = client_connection_stage::established;
    int32_t shard_id;  /// ID of server-side shard which is processing the connection.

    // `optional' column means that it's nullable (possibly because it's
    // unimplemented yet). If you want to fill ("implement") any of them,
    // remember to update the query in `notify_new_client()'.
    std::optional<sstring> driver_name;
    std::optional<sstring> driver_version;
    std::optional<sstring> hostname;
    std::optional<int32_t> protocol_version;
    std::optional<sstring> ssl_cipher_suite;
    std::optional<bool> ssl_enabled;
    std::optional<sstring> ssl_protocol;
    std::optional<sstring> username;
};

future<> notify_new_client(client_data cd);
future<> notify_disconnected_client(net::inet_address addr, int port, client_type ct);
future<> clear_clientlist();

template <changed_column column_enum_val>
struct notify_client_change {
    template <typename T>
    future<> operator()(net::inet_address addr, int port, client_type ct, T&& value) {
        const static sstring req
                = format("UPDATE system.{} SET {}=? WHERE address=? AND port=? AND client_type=?;",
                        db::system_keyspace_CLIENTS, column_literal<column_enum_val>);

        return db::qctx->execute_cql(req, std::forward<T>(value), std::move(addr), port, to_string(ct)).discard_result();
    }
};
