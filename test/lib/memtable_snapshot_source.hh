/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "mutation_reader.hh"
#include "memtable.hh"
#include "utils/phased_barrier.hh"
#include "test/lib/reader_permit.hh"
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/util/closeable.hh>

// in-memory snapshottable mutation source.
// Must be destroyed in a seastar thread.
class memtable_snapshot_source {
    schema_ptr _s;
    circular_buffer<lw_shared_ptr<memtable>> _memtables;
    utils::phased_barrier _apply;
    bool _closed = false;
    seastar::condition_variable _should_compact;
    future<> _compactor;
private:
    bool should_compact() const {
        return !_closed && _memtables.size() >= 3;
    }
    lw_shared_ptr<memtable> new_memtable() {
        return make_lw_shared<memtable>(_s);
    }
    lw_shared_ptr<memtable> pending() {
        if (_memtables.empty()) {
            _memtables.push_back(new_memtable());
            on_new_memtable();
        }
        return _memtables.back();
    }
    void on_new_memtable() {
        if (should_compact()) {
            _should_compact.signal();
        }
    }
    void compact() {
        if (_memtables.empty()) {
            return;
        }
        auto count = _memtables.size();
        auto op = _apply.start();
        auto new_mt = make_lw_shared<memtable>(_s);
        std::vector<flat_mutation_reader> readers;
        for (auto&& mt : _memtables) {
            readers.push_back(mt->make_flat_reader(new_mt->schema(),
                 tests::make_permit(),
                 query::full_partition_range,
                 new_mt->schema()->full_slice(),
                 default_priority_class(),
                 nullptr,
                 streamed_mutation::forwarding::no,
                 mutation_reader::forwarding::yes));
        }
        _memtables.push_back(new_memtable());
        auto&& rd = make_combined_reader(new_mt->schema(), tests::make_permit(), std::move(readers));
        auto close_rd = deferred_close(rd);
        consume_partitions(rd, [&] (mutation&& m) {
            new_mt->apply(std::move(m));
            return stop_iteration::no;
        }, db::no_timeout).get();
        _memtables.erase(_memtables.begin(), _memtables.begin() + count);
        _memtables.push_back(new_mt);
    }
public:
    memtable_snapshot_source(schema_ptr s)
        : _s(s)
        , _compactor(seastar::async([this] () noexcept {
            while (!_closed) {
                // condition_variable::wait() also allocates memory
                memory::scoped_critical_alloc_section dfg;
                _should_compact.wait().get();
                while (should_compact()) {
                    compact();
                }
            }
        }))
    { }
    memtable_snapshot_source(memtable_snapshot_source&&) = delete; // 'this' captured.
    ~memtable_snapshot_source() {
        _closed = true;
        _should_compact.broadcast();
        _compactor.get();
    }
    // Will cause subsequent apply() calls to accept writes conforming to given schema (or older).
    // Without this, the writes will be upgraded to the old schema and snapshots will not reflect
    // parts of writes which depend on the new schema.
    void set_schema(schema_ptr s) {
        pending()->set_schema(s);
        _s = s;
    }
    // Must run in a seastar thread
    void clear() {
        _memtables.erase(_memtables.begin(), _memtables.end());
        _apply.advance_and_await().get();
        _memtables.erase(_memtables.begin(), _memtables.end());
    }
    void apply(const mutation& mt) {
        pending()->apply(mt);
    }
    // Must run in a seastar thread
    void apply(memtable& mt) {
        auto op = _apply.start();
        auto new_mt = new_memtable();
        new_mt->apply(mt, tests::make_permit()).get();
        _memtables.push_back(new_mt);
    }
    // mt must not change from now on.
    void apply(lw_shared_ptr<memtable> mt) {
        auto op = _apply.start();
        _memtables.push_back(std::move(mt));
        on_new_memtable();
    }
    mutation_source operator()() {
        std::vector<mutation_source> src;
        for (auto&& mt : _memtables) {
            src.push_back(mt->as_data_source());
        }
        _memtables.push_back(new_memtable()); // so that src won't change any more.
        on_new_memtable();
        return make_combined_mutation_source(std::move(src));
    }
};
