/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <vector>
#include <sstream>
#include <unordered_set>
#include <set>
#include <experimental/optional>

#include "seastarx.hh"
#include "utils/chunked_vector.hh"

template<typename Iterator>
static inline
sstring join(sstring delimiter, Iterator begin, Iterator end) {
    std::ostringstream oss;
    while (begin != end) {
        oss << *begin;
        ++begin;
        if (begin != end) {
            oss << delimiter;
        }
    }
    return oss.str();
}

template<typename PrintableRange>
static inline
sstring join(sstring delimiter, const PrintableRange& items) {
    return join(delimiter, items.begin(), items.end());
}

namespace std {

template<typename Printable>
static inline
sstring
to_string(const std::vector<Printable>& items) {
    return "[" + join(", ", items) + "]";
}

template<typename Printable>
static inline
sstring
to_string(const std::set<Printable>& items) {
    return "{" + join(", ", items) + "}";
}

template<typename Printable>
static inline
sstring
to_string(const std::unordered_set<Printable>& items) {
    return "{" + join(", ", items) + "}";
}

template<typename Printable>
static inline
sstring
to_string(std::initializer_list<Printable> items) {
    return "[" + join(", ", std::begin(items), std::end(items)) + "]";
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& os, const std::pair<K, V>& p) {
    os << "{" << p.first << ", " << p.second << "}";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::unordered_set<T>& items) {
    os << "{" << join(", ", items) << "}";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::set<T>& items) {
    os << "{" << join(", ", items) << "}";
    return os;
}

template<typename T, size_t N>
std::ostream& operator<<(std::ostream& os, const std::array<T, N>& items) {
    os << "{" << join(", ", items) << "}";
    return os;
}

template <typename K, typename V, typename... Args>
std::ostream& operator<<(std::ostream& os, const std::unordered_map<K, V, Args...>& items) {
    os << "{" << join(", ", items) << "}";
    return os;
}

template <typename K, typename V, typename... Args>
std::ostream& operator<<(std::ostream& os, const std::map<K, V, Args...>& items) {
    os << "{" << join(", ", items) << "}";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const utils::chunked_vector<T>& items) {
    os << "[" << join(", ", items) << "]";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::list<T>& items) {
    os << "[" << join(", ", items) << "]";
    return os;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::optional<T>& opt) {
    if (opt) {
        os << "{" << *opt << "}";
    } else {
        os << "{}";
    }
    return os;
}

namespace experimental {

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::experimental::optional<T>& opt) {
    if (opt) {
        os << "{" << *opt << "}";
    } else {
        os << "{}";
    }
    return os;
}

}

}
