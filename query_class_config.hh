/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <cinttypes>

class reader_concurrency_semaphore;

struct query_class_config {
    reader_concurrency_semaphore& semaphore;
    uint64_t max_memory_for_unlimited_query;
};
