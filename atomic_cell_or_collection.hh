/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "atomic_cell.hh"
#include "schema.hh"
#include "hashing.hh"

// A variant type that can hold either an atomic_cell, or a serialized collection.
// Which type is stored is determined by the schema.
// Has an "empty" state.
// Objects moved-from are left in an empty state.
class atomic_cell_or_collection final {
    managed_bytes _data;
private:
    atomic_cell_or_collection(managed_bytes&& data) : _data(std::move(data)) {}
public:
    atomic_cell_or_collection() = default;
    atomic_cell_or_collection(atomic_cell ac) : _data(std::move(ac._data)) {}
    static atomic_cell_or_collection from_atomic_cell(atomic_cell data) { return { std::move(data._data) }; }
    atomic_cell_view as_atomic_cell() const { return atomic_cell_view::from_bytes(_data); }
    atomic_cell_ref as_atomic_cell_ref() { return { _data }; }
    atomic_cell_mutable_view as_mutable_atomic_cell() { return atomic_cell_mutable_view::from_bytes(_data); }
    atomic_cell_or_collection(collection_mutation cm) : _data(std::move(cm.data)) {}
    explicit operator bool() const {
        return !_data.empty();
    }
    bool can_use_mutable_view() const {
        return !_data.is_fragmented();
    }
    static atomic_cell_or_collection from_collection_mutation(collection_mutation data) {
        return std::move(data.data);
    }
    collection_mutation_view as_collection_mutation() const {
        return collection_mutation_view{_data};
    }
    bytes_view serialize() const {
        return _data;
    }
    bool operator==(const atomic_cell_or_collection& other) const {
        return _data == other._data;
    }
    template<typename Hasher>
    void feed_hash(Hasher& h, const column_definition& def) const {
        if (def.is_atomic()) {
            ::feed_hash(h, as_atomic_cell(), def);
        } else {
            ::feed_hash(h, as_collection_mutation(), def);
        }
    }
    size_t external_memory_usage() const {
        return _data.external_memory_usage();
    }
    friend std::ostream& operator<<(std::ostream&, const atomic_cell_or_collection&);
};
