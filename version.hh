
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/print.hh>
#include <tuple>

namespace version {
class version {
    std::tuple<uint16_t, uint16_t, uint16_t> _version;
public:
    version(uint16_t x, uint16_t y = 0, uint16_t z = 0): _version(std::make_tuple(x, y, z)) {}

    seastar::sstring to_sstring() {
        return seastar::format("{:d}.{:d}.{:d}", std::get<0>(_version), std::get<1>(_version), std::get<2>(_version));
    }

    static version current() {
        static version v(3, 0, 8);
        return v;
    }

    bool operator==(version v) const {
        return _version == v._version;
    }

    bool operator!=(version v) const {
        return _version != v._version;
    }

    bool operator<(version v) const {
        return _version < v._version;
    }
    bool operator<=(version v) {
        return _version <= v._version;
    }
    bool operator>(version v) {
        return _version > v._version;
    }
    bool operator>=(version v) {
        return _version >= v._version;
    }
};

inline const seastar::sstring& release() {
    static thread_local auto str_ver = version::current().to_sstring();
    return str_ver;
}
}
