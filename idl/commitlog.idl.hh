/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class commitlog_entry [[writable]] {
    std::experimental::optional<column_mapping> mapping();
    frozen_mutation mutation();
};
