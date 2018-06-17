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
#include "storage_helper.hh"
#include "db/config.hh"

namespace audit {

class audit_syslog_storage_helper : public storage_helper {
    int _syslog_fd;
public:
    audit_syslog_storage_helper() {};
    virtual ~audit_syslog_storage_helper();
    virtual future<> start(const db::config& cfg) override;
    virtual future<> stop() override;
    virtual future<> write(const audit_info* audit_info,
                           net::ipv4_address node_ip,
                           net::ipv4_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) override;
    virtual future<> write_login(const sstring& username,
                                 net::ipv4_address node_ip,
                                 net::ipv4_address client_ip,
                                 bool error) override;
};

}
