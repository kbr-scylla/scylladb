/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
future<> segregate_by_partition(flat_mutation_reader producer, segregate_config cfg, reader_consumer consumer);

} // namespace mutation_writer
