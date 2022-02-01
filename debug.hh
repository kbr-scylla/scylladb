/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/sharded.hh>

namespace replica {
class database;
}

namespace debug {

extern seastar::sharded<replica::database>* the_database;


}

