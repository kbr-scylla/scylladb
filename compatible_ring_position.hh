/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#pragma once

#include "query-request.hh"
#include <experimental/optional>

// Wraps ring_position so it is compatible with old-style C++: default constructor,
// stateless comparators, yada yada
class compatible_ring_position {
    const schema* _schema = nullptr;
    // optional to supply a default constructor, no more
    std::experimental::optional<dht::ring_position> _rp;
public:
    compatible_ring_position() noexcept = default;
    compatible_ring_position(const schema& s, const dht::ring_position& rp)
            : _schema(&s), _rp(rp) {
    }
    compatible_ring_position(const schema& s, dht::ring_position&& rp)
            : _schema(&s), _rp(std::move(rp)) {
    }
    const dht::token& token() const {
        return _rp->token();
    }
    friend int tri_compare(const compatible_ring_position& x, const compatible_ring_position& y) {
        return x._rp->tri_compare(*x._schema, *y._rp);
    }
    friend bool operator<(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) < 0;
    }
    friend bool operator<=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) <= 0;
    }
    friend bool operator>(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) > 0;
    }
    friend bool operator>=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) >= 0;
    }
    friend bool operator==(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) == 0;
    }
    friend bool operator!=(const compatible_ring_position& x, const compatible_ring_position& y) {
        return tri_compare(x, y) != 0;
    }
};

