/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once
#include <memory>

class flat_mutation_reader;

namespace query {
    struct max_result_size; 
    class partition_slice;
}


/// A reader that emits partitions in native reverse order.
///
/// 1. The reader's schema() method will return a reversed schema (see
///    \ref schema::make_reversed()).
/// 2. Static row is still emitted first.
/// 3. Range tombstones' bounds are reversed (see \ref range_tombstone::reverse()).
/// 4. Clustered rows and range tombstones are emitted in descending order.
/// Because of 3 and 4 the guarantee that a range tombstone is emitted before
/// any mutation fragment affected by it still holds.
/// Ordering of partitions themselves remains unchanged.
/// For more details see docs/design-notes/reverse-reads.md.
///
/// The reader's schema (returned by `schema()`) is the reverse of `original`'s schema.
///
/// \param original the reader to be reversed.
/// \param max_size the maximum amount of memory the reader is allowed to use
///     for reversing and conversely the maximum size of the results. The
///     reverse reader reads entire partitions into memory, before reversing
///     them. Since partitions can be larger than the available memory, we need
///     to enforce a limit on memory consumption. When reaching the soft limit
///     a warning will be logged. When reaching the hard limit the read will be
///     aborted.
/// \param slice serves as a convenience slice storage for reads that have to
///     store an edited slice somewhere. This is common for reads that work
///     with a native-reversed slice and so have to convert the one used in the
///     query -- which is in half-reversed format.
///
/// FIXME: reversing should be done in the sstable layer, see #1413.
flat_mutation_reader
make_reversing_reader(flat_mutation_reader original, query::max_result_size max_size, std::unique_ptr<query::partition_slice> slice = {});
