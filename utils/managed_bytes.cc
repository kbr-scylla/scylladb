
/*
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include "managed_bytes.hh"

thread_local managed_bytes::linearization_context managed_bytes::_linearization_context;
thread_local std::unordered_map<const blob_storage*, std::unique_ptr<bytes_view::value_type[]>> managed_bytes::_lc_state;

void
managed_bytes::linearization_context::forget(const blob_storage* p) noexcept {
    _lc_state.erase(p);
}

const bytes_view::value_type*
managed_bytes::do_linearize() const {
    auto& lc = _linearization_context;
    assert(lc._nesting);
    lc._state_ptr = &_lc_state;
    auto b = _u.ptr;
    auto i = _lc_state.find(b);
    if (i == _lc_state.end()) {
        auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[b->size]);
        auto e = data.get();
        while (b) {
            e = std::copy_n(b->data, b->frag_size, e);
            b = b->next;
        }
        i = _lc_state.emplace(_u.ptr, std::move(data)).first;
    }
    return i->second.get();
}

