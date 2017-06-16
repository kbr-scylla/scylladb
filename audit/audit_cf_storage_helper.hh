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
#include "table_helper.hh"
#include "storage_helper.hh"

namespace audit {

class audit_cf_storage_helper : public storage_helper {
    static const sstring KEYSPACE_NAME;
    static const sstring TABLE_NAME;
    table_helper _table;
    service::query_state _dummy_query_state;
    static cql3::query_options make_data(const audit_info* audit_info,
                                         net::ipv4_address node_ip,
                                         net::ipv4_address client_ip,
                                         db::consistency_level cl,
                                         const sstring& username,
                                         bool error);
public:
    audit_cf_storage_helper();
    virtual ~audit_cf_storage_helper() {}
    virtual future<> start() override;
    virtual future<> stop() override;
    virtual future<> write(const audit_info* audit_info,
                           net::ipv4_address node_ip,
                           net::ipv4_address client_ip,
                           db::consistency_level cl,
                           const sstring& username,
                           bool error) override;
};

}
