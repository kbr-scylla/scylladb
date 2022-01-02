/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "schema_fwd.hh"
#include "query-request.hh"

namespace query {

class clustering_key_filter_ranges {
    clustering_row_ranges _storage;
    std::reference_wrapper<const clustering_row_ranges> _ref;
public:
    clustering_key_filter_ranges(const clustering_row_ranges& ranges) : _ref(ranges) { }
    clustering_key_filter_ranges(clustering_row_ranges&& ranges)
        : _storage(std::make_move_iterator(ranges.begin()), std::make_move_iterator(ranges.end())), _ref(_storage) {}

    struct reversed { };
    clustering_key_filter_ranges(reversed, const clustering_row_ranges& ranges)
        : _storage(ranges.rbegin(), ranges.rend()), _ref(_storage) { }

    clustering_key_filter_ranges(clustering_key_filter_ranges&& other) noexcept
        : _storage(std::move(other._storage))
        , _ref(&other._ref.get() == &other._storage ? _storage : other._ref.get())
    { }

    clustering_key_filter_ranges& operator=(clustering_key_filter_ranges&& other) noexcept {
        if (this != &other) {
            _storage = std::move(other._storage);
            _ref = (&other._ref.get() == &other._storage) ? _storage : other._ref.get();
        }
        return *this;
    }

    auto begin() const { return _ref.get().begin(); }
    auto end() const { return _ref.get().end(); }
    bool empty() const { return _ref.get().empty(); }
    size_t size() const { return _ref.get().size(); }
    const clustering_row_ranges& ranges() const { return _ref; }

    // Returns all clustering ranges determined by `slice` inside partition determined by `key`.
    // If the slice contains the `reversed` option, we assume that it is given in 'half-reversed' format
    // (i.e. the ranges within are given in reverse order, but the ranges themselves are not reversed)
    // with respect to the table order.
    // The ranges will be returned in forward (increasing) order even if the slice is reversed.
    static clustering_key_filter_ranges get_ranges(const schema& schema, const query::partition_slice& slice, const partition_key& key) {
        const query::clustering_row_ranges& ranges = slice.row_ranges(schema, key);
        if (slice.is_reversed()) {
            return clustering_key_filter_ranges(clustering_key_filter_ranges::reversed{}, ranges);
        }
        return clustering_key_filter_ranges(ranges);
    }

    // Returns all clustering ranges determined by `slice` inside partition determined by `key`.
    // The ranges will be returned in the same order as stored in the slice.
    static clustering_key_filter_ranges get_native_ranges(const schema& schema, const query::partition_slice& slice, const partition_key& key) {
        const query::clustering_row_ranges& ranges = slice.row_ranges(schema, key);
        return clustering_key_filter_ranges(ranges);
    }
};

}
