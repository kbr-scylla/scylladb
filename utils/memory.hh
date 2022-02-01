/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <cstddef>

// return total shard's memory available for general operation
// it may be different from total available memory if some memory
// is reserved for special use
size_t get_available_memory();

// reserve memory that will not be reported in total
void reserve_memory(size_t);
