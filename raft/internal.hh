/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <ostream>
#include <functional>
#include "utils/UUID.hh"

namespace raft {
namespace internal {

template<typename Tag>
class tagged_uint64 {
    uint64_t _val;
public:
    tagged_uint64() : _val(0) {}
    explicit tagged_uint64(uint64_t v) : _val(v) {}
    tagged_uint64(const tagged_uint64&) = default;
    tagged_uint64(tagged_uint64&&) = default;
    tagged_uint64& operator=(const tagged_uint64&) = default;
    auto operator<=>(const tagged_uint64&) const = default;
    explicit operator bool() const { return _val != 0; }

    uint64_t get_value() const {
        return _val;
    }
    operator uint64_t() const {
        return get_value();
    }
    tagged_uint64& operator++() { // pre increment
        ++_val;
        return *this;
    }
    tagged_uint64 operator++(int) { // post increment
        uint64_t v = _val++;
        return tagged_uint64(v);
    }
    tagged_uint64& operator--() { // pre decrement
        --_val;
        return *this;
    }
    tagged_uint64 operator--(int) { // post decrement
        uint64_t v = _val--;
        return tagged_uint64(v);
    }
    tagged_uint64 operator+(const tagged_uint64& o) const {
        return tagged_uint64(_val + o._val);
    }
    tagged_uint64 operator-(const tagged_uint64& o) const {
        return tagged_uint64(_val - o._val);
    }
    friend std::ostream& operator<<(std::ostream& os, const tagged_uint64<Tag>& u) {
        os << u._val;
        return os;
    }
};

template<typename Tag>
struct tagged_id {
    utils::UUID id;
    bool operator==(const tagged_id& o) const {
        return id == o.id;
    }
    bool operator<(const tagged_id& o) const {
        return id < o.id;
    }
    explicit operator bool() const {
        // The default constructor sets the id to nil, which is
        // guaranteed to not match any valid id.
        return id != utils::UUID();
    }
    static tagged_id create_random_id() { return tagged_id{utils::make_random_uuid()}; }
    explicit tagged_id(const utils::UUID& uuid) : id(uuid) {}
    tagged_id() = default;
};

template<typename Tag>
std::ostream& operator<<(std::ostream& os, const tagged_id<Tag>& id) {
    os << id.id;
    return os;
}

} // end of namespace internal
} // end of namespace raft

namespace std {

template<typename Tag>
struct hash<raft::internal::tagged_id<Tag>> {
    size_t operator()(const raft::internal::tagged_id<Tag>& id) const {
        return hash<utils::UUID>()(id.id);
    }
};

template<typename Tag>
struct hash<raft::internal::tagged_uint64<Tag>> {
    size_t operator()(const raft::internal::tagged_uint64<Tag>& val) const {
        return hash<uint64_t>()(val);
    }
};

} // end of namespace std

