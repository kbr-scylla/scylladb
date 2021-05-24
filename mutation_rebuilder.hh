/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "mutation.hh"

class mutation_rebuilder {
    mutation _m;

public:
    mutation_rebuilder(dht::decorated_key dk, schema_ptr s)
        : _m(std::move(s), std::move(dk)) {
    }

    stop_iteration consume(tombstone t) {
        _m.partition().apply(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        _m.partition().apply_row_tombstone(*_m.schema(), std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        _m.partition().static_row().apply(*_m.schema(), column_kind::static_column, std::move(sr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        auto& dr = _m.partition().clustered_row(*_m.schema(), std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(*_m.schema(), column_kind::regular_column, std::move(cr.cells()));
        return stop_iteration::no;
    }

    mutation_opt consume_end_of_stream() {
        return mutation_opt(std::move(_m));
    }
};
