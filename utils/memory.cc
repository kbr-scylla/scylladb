/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "utils/memory.hh"
#include "seastar/core/memory.hh"

static thread_local size_t reserved_memory;

size_t get_available_memory() {
    return seastar::memory::stats().total_memory() - reserved_memory;
}

void reserve_memory(size_t reserve) {
    if (seastar::memory::stats().total_memory() <= reserved_memory + reserve) {
        throw std::runtime_error("Trying to reserve to much memory");
    }
    reserved_memory += reserve;
}
