/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class cql_serialization_format final {
    uint8_t protocol_version();
};

namespace query {

class specific_ranges {
    partition_key pk();
    std::vector<nonwrapping_range<clustering_key_prefix>> ranges();
};

class partition_slice {
    std::vector<nonwrapping_range<clustering_key_prefix>> default_row_ranges();
    utils::small_vector<uint32_t, 8> static_columns;
    utils::small_vector<uint32_t, 8> regular_columns;
    query::partition_slice::option_set options;
    std::unique_ptr<query::specific_ranges> get_specific_ranges();
    cql_serialization_format cql_format();
    uint32_t partition_row_limit() [[version 1.3]] = std::numeric_limits<uint32_t>::max();
};

class read_command {
    utils::UUID cf_id;
    utils::UUID schema_version;
    query::partition_slice slice;
    uint32_t row_limit;
    std::chrono::time_point<gc_clock, gc_clock::duration> timestamp;
    std::optional<tracing::trace_info> trace_info [[version 1.3]];
    uint32_t partition_limit [[version 1.3]] = std::numeric_limits<uint32_t>::max();
    utils::UUID query_uuid [[version 2.2]] = utils::UUID();
    query::is_first_page is_first_page [[version 2.2]] = query::is_first_page::no;
};

}
