/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "memtable.hh"
#include "row_cache.hh"
#include "dirty_memory_manager.hh"

// makes sure that cache update handles real dirty memory correctly.
class real_dirty_memory_accounter {
    dirty_memory_manager& _mgr;
    cache_tracker& _tracker;
    uint64_t _bytes;
    uint64_t _uncommitted = 0;
public:
    real_dirty_memory_accounter(dirty_memory_manager& mgr, cache_tracker& tracker, size_t size);
    real_dirty_memory_accounter(memtable& m, cache_tracker& tracker);
    ~real_dirty_memory_accounter();
    real_dirty_memory_accounter(real_dirty_memory_accounter&& c);
    real_dirty_memory_accounter(const real_dirty_memory_accounter& c) = delete;
    // Needs commit() to take effect, or when this object is destroyed.
    void unpin_memory(uint64_t bytes) { _uncommitted += bytes; }
    void commit();
};

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(dirty_memory_manager& mgr, cache_tracker& tracker, size_t size)
    : _mgr(mgr)
    , _tracker(tracker)
    , _bytes(size) {
    _mgr.pin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(memtable& m, cache_tracker& tracker)
    : real_dirty_memory_accounter(m.get_dirty_memory_manager(), tracker, m.occupancy().used_space())
{ }

inline
real_dirty_memory_accounter::~real_dirty_memory_accounter() {
    _mgr.unpin_real_dirty_memory(_bytes);
}

inline
real_dirty_memory_accounter::real_dirty_memory_accounter(real_dirty_memory_accounter&& c)
    : _mgr(c._mgr), _tracker(c._tracker), _bytes(c._bytes), _uncommitted(c._uncommitted) {
    c._bytes = 0;
    c._uncommitted = 0;
}

inline
void real_dirty_memory_accounter::commit() {
    auto bytes = std::exchange(_uncommitted, 0);
    // this should never happen - if it does it is a bug. But we'll try to recover and log
    // instead of asserting. Once it happens, though, it can keep happening until the update is
    // done. So using metrics is better-suited than printing to the logs
    if (bytes > _bytes) {
        _tracker.pinned_dirty_memory_overload(bytes - _bytes);
    }
    auto delta = std::min(bytes, _bytes);
    _bytes -= delta;
    _mgr.unpin_real_dirty_memory(delta);
}
