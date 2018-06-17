/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace query {

class result_digest final {
    std::array<uint8_t, 16> get();
};

class result {
    bytes buf();
    std::experimental::optional<query::result_digest> digest();
    api::timestamp_type last_modified() [ [version 1.2] ] = api::missing_timestamp;
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
    std::experimental::optional<uint32_t> row_count() [[version 2.1]];
    std::experimental::optional<uint32_t> partition_count() [[version 2.1]];
};

}
