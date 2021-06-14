
/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include "managed_bytes.hh"

std::unique_ptr<bytes_view::value_type[]>
managed_bytes::do_linearize_pure() const {
    auto b = _u.ptr;
    auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[b->size]);
    auto e = data.get();
    while (b) {
        e = std::copy_n(b->data, b->frag_size, e);
        b = b->next;
    }
    return data;
}

sstring to_hex(const managed_bytes& b) {
    return to_hex(managed_bytes_view(b));
}

sstring to_hex(const managed_bytes_opt& b) {
    return !b ? "null" : to_hex(*b);
}

std::ostream& operator<<(std::ostream& os, const managed_bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "null";
}
