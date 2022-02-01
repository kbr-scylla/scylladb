/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "flat_mutation_reader_v2.hh"
#include "sstables/progress_monitor.hh"

namespace sstables {
namespace mx {

// Precondition: if the slice is reversed, the schema must be reversed as well
// and the range must be singular (`range.is_singular()`).
// Reversed slices must be provided in the 'half-reversed' format (the order of ranges
// being reversed, but the ranges themselves are not).
// Fast-forwarding is not supported in reversed queries (FIXME).
flat_mutation_reader_v2 make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor);

// Same as above but the slice is moved and stored inside the reader.
flat_mutation_reader_v2 make_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        query::partition_slice&& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        read_monitor& monitor);

// A reader which doesn't use the index at all. It reads everything from the
// sstable and it doesn't support skipping.
flat_mutation_reader_v2 make_crawling_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor);

} // namespace mx
} // namespace sstables
