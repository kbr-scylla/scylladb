/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

// FIXME: FNV-1a is quite slow, consider something faster, CityHash seems to be
// a good choice.

template<unsigned>
struct fnv1a_constants { };

template<>
struct fnv1a_constants<8> {
    enum : uint64_t {
        offset = 0xcbf29ce484222325ull,
        prime = 0x100000001b3ull,
    };
};

class fnv1a_hasher {
    using constants = fnv1a_constants<sizeof(size_t)>;
    size_t _hash = constants::offset;
public:
    void update(const char* ptr, size_t length) {
        auto end = ptr + length;
        while (ptr != end) {
            _hash ^= *ptr;
            _hash *= constants::prime;
            ++ptr;
        }
    }

    size_t finalize() const {
        return _hash;
    }
};
