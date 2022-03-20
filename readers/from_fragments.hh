/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once
#include "schema_fwd.hh"
#include <deque>
#include "dht/i_partitioner_fwd.hh"

class flat_mutation_reader;
class reader_permit;
class mutation_fragment;
class ring_position;

namespace query {
    class partition_slice;
}

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>);

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>, const dht::partition_range& pr);

flat_mutation_reader
make_flat_mutation_reader_from_fragments(schema_ptr, reader_permit, std::deque<mutation_fragment>, const dht::partition_range& pr, const query::partition_slice& slice);

