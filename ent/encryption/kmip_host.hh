/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <vector>
#include <optional>
#include <chrono>
#include <iosfwd>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>

#include "../../bytes.hh"

#include "symmetric_key.hh"

namespace encryption {

class symmetric_key;
class encryption_context;
struct key_info;

class kmip_host {
public:
    struct host_options {
        std::vector<sstring> hosts;

        sstring username;
        sstring password;

        sstring certfile;
        sstring keyfile;
        sstring truststore;
        sstring priority_string;

        std::optional<std::chrono::milliseconds> key_cache_expiry;
        std::optional<size_t> max_pooled_connections_per_host;
        std::optional<size_t> max_command_retries;
    };
    struct key_options {
        sstring template_name;
        sstring key_namespace;
    };
    using id_type = bytes;

    kmip_host(encryption_context&, const sstring& name, const host_options&);
    kmip_host(encryption_context&, const sstring& name, const std::unordered_map<sstring, sstring>&);
    ~kmip_host();

    future<> connect();
    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const key_options& = {});
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, std::optional<key_info> = std::nullopt);

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

std::ostream& operator<<(std::ostream&, const kmip_host::key_options&);

}
