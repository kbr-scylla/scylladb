/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "mutation_fragment.hh"
#include "converting_mutation_partition_applier.hh"

// A StreamedMutationTransformer which transforms the stream to a different schema
class schema_upgrader {
    schema_ptr _prev;
    schema_ptr _new;
private:
    row transform(row&& r, column_kind kind) {
        row new_row;
        r.for_each_cell([&] (column_id id, atomic_cell_or_collection& cell) {
            const column_definition& col = _prev->column_at(kind, id);
            const column_definition* new_col = _new->get_column_definition(col.name());
            if (new_col) {
                converting_mutation_partition_applier::append_cell(new_row, kind, *new_col, col, std::move(cell));
            }
        });
        return new_row;
    }
public:
    schema_upgrader(schema_ptr s)
        : _new(std::move(s))
    { }
    schema_ptr operator()(schema_ptr old) {
        _prev = std::move(old);
        return _new;
    }
    mutation_fragment consume(static_row&& row) {
        return mutation_fragment(static_row(transform(std::move(row.cells()), column_kind::static_column)));
    }
    mutation_fragment consume(clustering_row&& row) {
        return mutation_fragment(clustering_row(row.key(), row.tomb(), row.marker(),
            transform(std::move(row.cells()), column_kind::regular_column)));
    }
    mutation_fragment consume(range_tombstone&& rt) {
        return std::move(rt);
    }
    mutation_fragment consume(partition_start&& ph) {
        return std::move(ph);
    }
    mutation_fragment consume(partition_end&& eop) {
        return std::move(eop);
    }
    mutation_fragment operator()(mutation_fragment&& mf) {
        return std::move(mf).consume(*this);
    }
};

GCC6_CONCEPT(
static_assert(StreamedMutationTranformer<schema_upgrader>());
)
