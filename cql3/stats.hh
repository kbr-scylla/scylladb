/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

namespace cql3 {

struct cql_stats {
    uint64_t reads = 0;
    uint64_t inserts = 0;
    uint64_t updates = 0;
    uint64_t deletes = 0;
    uint64_t batches = 0;
    uint64_t statements_in_batches = 0;
    uint64_t batches_pure_logged = 0;
    uint64_t batches_pure_unlogged = 0;
    uint64_t batches_unlogged_from_logged = 0;
};

}
