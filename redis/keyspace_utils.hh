/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastar/core/sharded.hh"
#include "seastar/core/future.hh"

namespace service {
class migration_manager;
}
namespace db {
class config;
}

namespace gms {
class gossiper;
}

namespace redis {

static constexpr auto DATA_COLUMN_NAME = "data";
static constexpr auto STRINGs         = "STRINGs";
static constexpr auto LISTs           = "LISTs";
static constexpr auto HASHes          = "HASHes";
static constexpr auto SETs            = "SETs";
static constexpr auto ZSETs           = "ZSETs";

seastar::future<> maybe_create_keyspace(seastar::sharded<service::migration_manager>& mm, db::config& cfg, seastar::sharded<gms::gossiper>& g);

}
