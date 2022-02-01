/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

class commitlog_entry [[writable]] {
    std::optional<column_mapping> mapping();
    frozen_mutation mutation();
};
