/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <ostream>

namespace gms {

enum class application_state {
    STATUS = 0,
    LOAD,
    SCHEMA,
    DC,
    RACK,
    RELEASE_VERSION,
    REMOVAL_COORDINATOR,
    INTERNAL_IP,
    RPC_ADDRESS,
    X_11_PADDING, // padding specifically for 1.1
    SEVERITY,
    NET_VERSION,
    HOST_ID,
    TOKENS,
    SUPPORTED_FEATURES,
    CACHE_HITRATES,
    SCHEMA_TABLES_VERSION,
    RPC_READY,
    VIEW_BACKLOG,
    SHARD_COUNT,
    IGNORE_MSB_BITS,
    CDC_GENERATION_ID,
    SNITCH_NAME,
    // pad to allow adding new states to existing cluster
    X10,
};

std::ostream& operator<<(std::ostream& os, const application_state& m);

}
