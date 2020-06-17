/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "sstables/shared_sstable.hh"
#include "database.hh"

namespace db::view {

/*
 * A consumer that pushes materialized view updates for each consumed mutation.
 * It is expected to be run in seastar::async threaded context through consume_in_thread()
 */
class view_updating_consumer {
    schema_ptr _schema;
    lw_shared_ptr<table> _table;
    std::vector<sstables::shared_sstable> _excluded_sstables;
    const seastar::abort_source* _as;
    std::optional<mutation> _m;
public:
    view_updating_consumer(schema_ptr schema, table& table, std::vector<sstables::shared_sstable> excluded_sstables, const seastar::abort_source& as)
            : _schema(std::move(schema))
            , _table(table.shared_from_this())
            , _excluded_sstables(std::move(excluded_sstables))
            , _as(&as)
            , _m()
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _m = mutation(_schema, dk, mutation_partition(_schema));
    }

    void consume(tombstone t) {
        _m->partition().apply(std::move(t));
    }

    stop_iteration consume(static_row&& sr) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(sr));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(cr));
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        if (_as->abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(rt));
        return stop_iteration::no;
    }

    // Expected to be run in seastar::async threaded context (consume_in_thread())
    stop_iteration consume_end_of_partition();

    stop_iteration consume_end_of_stream() {
        return stop_iteration(_as->abort_requested());
    }
};

}

