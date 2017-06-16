/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include "audit/audit.hh"
#include <seastar/core/future.hh>

namespace audit {

class storage_helper {
public:
    storage_helper() {}
    virtual ~storage_helper() {}
    virtual future<> start() = 0;
    virtual future<> stop() = 0;
    virtual future<> write(const audit_info* audit_info,
                           net::ipv4_address node_ip,
                           net::ipv4_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) = 0;
};

}
