/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <iostream>

#include "keys.hh"
#include "dht/i_partitioner.hh"
#include "clustering_bounds_comparator.hh"
#include <boost/algorithm/string.hpp>
#include <boost/any.hpp>

std::ostream& operator<<(std::ostream& out, const partition_key& pk) {
    return out << "pk{" << to_hex(managed_bytes_view(pk.representation())) << "}";
}

template<typename T>
static std::ostream& print_key(std::ostream& out, const T& key_with_schema) {
    const auto& [schema, key] = key_with_schema;
    auto type_iterator = key.get_compound_type(schema)->types().begin();
    bool first = true;
    for (auto&& e : key.components(schema)) {
        if (!first) {
            out << ":";
        }
        first = false;
        out << (*type_iterator)->to_string(to_bytes(e));
        ++type_iterator;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const partition_key::with_schema_wrapper& pk) {
    return print_key(out, pk);
}

std::ostream& operator<<(std::ostream& out, const clustering_key_prefix::with_schema_wrapper& ck) {
    return print_key(out, ck);
}

std::ostream& operator<<(std::ostream& out, const partition_key_view& pk) {
    return with_linearized(pk.representation(), [&] (bytes_view v) {
        return std::ref(out << "pk{" << to_hex(v) << "}");
    });
}

std::ostream& operator<<(std::ostream& out, const clustering_key_prefix& ckp) {
    return out << "ckp{" << to_hex(managed_bytes_view(ckp.representation())) << "}";
}

const legacy_compound_view<partition_key_view::c_type>
partition_key_view::legacy_form(const schema& s) const {
    return { *get_compound_type(s), _bytes };
}

std::strong_ordering
partition_key_view::legacy_tri_compare(const schema& s, partition_key_view o) const {
    auto cmp = legacy_compound_view<c_type>::tri_comparator(*get_compound_type(s));
    return cmp(this->representation(), o.representation());
}

std::strong_ordering
partition_key_view::ring_order_tri_compare(const schema& s, partition_key_view k2) const {
    auto t1 = dht::get_token(s, *this);
    auto t2 = dht::get_token(s, k2);
    if (t1 != t2) {
        return t1 < t2 ? std::strong_ordering::less : std::strong_ordering::greater;
    }
    return legacy_tri_compare(s, k2);
}

partition_key partition_key::from_nodetool_style_string(const schema_ptr s, const sstring& key) {
    std::vector<sstring> vec;
    boost::split(vec, key, boost::is_any_of(":"));

    auto it = std::begin(vec);
    if (vec.size() != s->partition_key_type()->types().size()) {
        throw std::invalid_argument("partition key '" + key + "' has mismatch number of components");
    }
    std::vector<bytes> r;
    r.reserve(vec.size());
    for (auto t : s->partition_key_type()->types()) {
        r.emplace_back(t->from_string(*it++));
    }
    return partition_key::from_range(std::move(r));
}

std::ostream& operator<<(std::ostream& out, const bound_kind k) {
    switch (k) {
    case bound_kind::excl_end:
        return out << "excl end";
    case bound_kind::incl_start:
        return out << "incl start";
    case bound_kind::incl_end:
        return out << "incl end";
    case bound_kind::excl_start:
        return out << "excl start";
    }
    abort();
}

bound_kind invert_kind(bound_kind k) {
    switch (k) {
    case bound_kind::excl_start: return bound_kind::incl_end;
    case bound_kind::incl_start: return bound_kind::excl_end;
    case bound_kind::excl_end:   return bound_kind::incl_start;
    case bound_kind::incl_end:   return bound_kind::excl_start;
    }
    abort();
}

int32_t weight(bound_kind k) {
    switch (k) {
    case bound_kind::excl_end:
        return -2;
    case bound_kind::incl_start:
        return -1;
    case bound_kind::incl_end:
        return 1;
    case bound_kind::excl_start:
        return 2;
    }
    abort();
}

const thread_local clustering_key_prefix bound_view::_empty_prefix = clustering_key::make_empty();
