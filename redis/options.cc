/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "redis/options.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "schema.hh"
#include "replica/database.hh"
#include <seastar/core/print.hh>
#include "redis/keyspace_utils.hh"

using namespace seastar;

namespace redis {

schema_ptr get_schema(service::storage_proxy& proxy, const sstring& ks_name, const sstring& cf_name) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(ks_name, cf_name);
    return schema;
}

}
