/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastarx.hh"
#include "core/sstring.hh"
#include "hashing.hh"
#include <experimental/optional>
#include <iosfwd>
#include <functional>
#include "utils/mutable_view.hh"

using bytes = basic_sstring<int8_t, uint32_t, 31>;
using bytes_view = std::experimental::basic_string_view<int8_t>;
using bytes_mutable_view = basic_mutable_view<bytes_view::value_type>;
using bytes_opt = std::experimental::optional<bytes>;
using sstring_view = std::experimental::string_view;

namespace std {

template <>
struct hash<bytes_view> {
    size_t operator()(bytes_view v) const {
        return hash<sstring_view>()({reinterpret_cast<const char*>(v.begin()), v.size()});
    }
};

}

bytes from_hex(sstring_view s);
sstring to_hex(bytes_view b);
sstring to_hex(const bytes& b);
sstring to_hex(const bytes_opt& b);

std::ostream& operator<<(std::ostream& os, const bytes& b);
std::ostream& operator<<(std::ostream& os, const bytes_opt& b);

namespace std {

// Must be in std:: namespace, or ADL fails
std::ostream& operator<<(std::ostream& os, const bytes_view& b);

}

template<>
struct appending_hash<bytes> {
    template<typename Hasher>
    void operator()(Hasher& h, const bytes& v) const {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.cbegin()), v.size() * sizeof(bytes::value_type));
    }
};

template<>
struct appending_hash<bytes_view> {
    template<typename Hasher>
    void operator()(Hasher& h, bytes_view v) const {
        feed_hash(h, v.size());
        h.update(reinterpret_cast<const char*>(v.begin()), v.size() * sizeof(bytes_view::value_type));
    }
};
