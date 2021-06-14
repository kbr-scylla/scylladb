/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "counters.hh"
#include "commitlog_entry.hh"
#include "idl/uuid.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/commitlog.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/commitlog.dist.impl.hh"

#include <seastar/core/simple-stream.hh>

template<typename Output>
void commitlog_entry_writer::serialize(Output& out) const {
    [this, wr = ser::writer_of_commitlog_entry<Output>(out)] () mutable {
        if (_with_schema) {
            return std::move(wr).write_mapping(_schema->get_column_mapping());
        } else {
            return std::move(wr).skip_mapping();
        }
    }().write_mutation(_mutation).end_commitlog_entry();
}

void commitlog_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
}

void commitlog_entry_writer::write(typename seastar::memory_output_stream<std::vector<temporary_buffer<char>>::iterator>& out) const {
    serialize(out);
}

commitlog_entry_reader::commitlog_entry_reader(const fragmented_temporary_buffer& buffer)
    : _ce([&] {
    auto in = seastar::fragmented_memory_input_stream(fragmented_temporary_buffer::view(buffer).begin(), buffer.size_bytes());
    return ser::deserialize(in, boost::type<commitlog_entry>());
}())
{
}
