/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "readers/flat_mutation_reader.hh"
#include "sstables/progress_monitor.hh"
#include <seastar/core/io_priority_class.hh>

namespace sstables {
namespace kl {

flat_mutation_reader make_reader(
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

// A reader which doesn't use the index at all. It reads everything from the
// sstable and it doesn't support skipping.
flat_mutation_reader make_crawling_reader(
        shared_sstable sstable,
        schema_ptr schema,
        reader_permit permit,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        read_monitor& monitor);

} // namespace kl
} // namespace sstables
