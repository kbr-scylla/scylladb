/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace tracing {
enum class trace_type : uint8_t {
    NONE,
    QUERY,
    REPAIR,
};

class span_id {
    uint64_t get_id();
};

class trace_info {
    utils::UUID session_id;
    tracing::trace_type type;
    bool write_on_close;
    tracing::trace_state_props_set state_props [[version 1.4]];
    uint32_t slow_query_threshold_us [[version 1.4]];
    uint32_t slow_query_ttl_sec [[version 1.4]];
    tracing::span_id parent_id [[version 1.8]]; /// RPC sender's tracing session span ID
};
}

