/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "schema.hh"
#include "query-request.hh"

namespace query {

class clustering_key_filter_ranges {
    clustering_row_ranges _storage;
    const clustering_row_ranges& _ref;
public:
    clustering_key_filter_ranges(const clustering_row_ranges& ranges) : _ref(ranges) { }
    struct reversed { };
    clustering_key_filter_ranges(reversed, const clustering_row_ranges& ranges)
        : _storage(ranges.rbegin(), ranges.rend()), _ref(_storage) { }

    clustering_key_filter_ranges(clustering_key_filter_ranges&& other) noexcept
        : _storage(std::move(other._storage))
        , _ref(&other._ref == &other._storage ? _storage : other._ref)
    { }

    clustering_key_filter_ranges& operator=(clustering_key_filter_ranges&& other) noexcept {
        if (this != &other) {
            this->~clustering_key_filter_ranges();
            new (this) clustering_key_filter_ranges(std::move(other));
        }
        return *this;
    }

    auto begin() const { return _ref.begin(); }
    auto end() const { return _ref.end(); }
    bool empty() const { return _ref.empty(); }
    size_t size() const { return _ref.size(); }
    const clustering_row_ranges& ranges() const { return _ref; }

    static clustering_key_filter_ranges get_ranges(const schema& schema, const query::partition_slice& slice, const partition_key& key) {
        const query::clustering_row_ranges& ranges = slice.row_ranges(schema, key);
        if (slice.options.contains(query::partition_slice::option::reversed)) {
            return clustering_key_filter_ranges(clustering_key_filter_ranges::reversed{}, ranges);
        }
        return clustering_key_filter_ranges(ranges);
    }
};

}
