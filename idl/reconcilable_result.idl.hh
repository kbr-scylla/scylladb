/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class partition {
    uint32_t row_count();
    frozen_mutation mut();
};

class reconcilable_result {
    uint32_t row_count();
    std::vector<partition> partitions();
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
};
