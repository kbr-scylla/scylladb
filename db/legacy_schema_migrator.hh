/*
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "seastarx.hh"

namespace replica {
class database;
}

namespace cql3 {
class query_processor;
}

namespace service {
class storage_proxy;
}

namespace db {
namespace legacy_schema_migrator {

future<> migrate(sharded<service::storage_proxy>&, sharded<replica::database>& db, cql3::query_processor&);

}
}
