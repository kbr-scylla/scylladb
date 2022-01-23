/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>

#include "feed_writers.hh"

namespace mutation_writer {

struct segregate_config {
    // For flushing the memtable which does the in-memory segregation (sorting)
    // part.
    const io_priority_class& pc;
    // Maximum amount of memory to be used by the in-memory segregation
    // (sorting) structures. Partitions can be split across partitions
    size_t max_memory;
};

// Given a producer that may contain partitions in the wrong order, or even
// contain partitions multiple times, separate them such that each output
// stream keeps the partition ordering guarantee. In other words, repair
// a stream that violates the ordering requirements by splitting it into output
// streams that honor it.
// This is useful for scrub compaction to split sstables containing out-of-order
// and/or duplicate partitions into sstables that honor the partition ordering.
future<> segregate_by_partition(flat_mutation_reader_v2 producer, segregate_config cfg, reader_consumer_v2 consumer);

} // namespace mutation_writer
