/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/select_statement.hh"
#include "dht/i_partitioner.hh"
#include "query-request.hh"
#include "schema.hh"

class view_info final {
    const schema& _schema;
    raw_view_info _raw;
    // The following fields are used to select base table rows.
    mutable shared_ptr<cql3::statements::select_statement> _select_statement;
    mutable stdx::optional<query::partition_slice> _partition_slice;
    mutable stdx::optional<dht::partition_range_vector> _partition_ranges;
    // Lazily initializes the column id of a regular base table included in the view's PK, if any.
    mutable stdx::optional<stdx::optional<column_id>> _base_non_pk_column_in_view_pk;
public:
    view_info(const schema& schema, const raw_view_info& raw_view_info);

    const raw_view_info& raw() const {
        return _raw;
    }

    const utils::UUID& base_id() const {
        return _raw.base_id();
    }

    const sstring& base_name() const {
        return _raw.base_name();
    }

    bool include_all_columns() const {
        return _raw.include_all_columns();
    }

    const sstring& where_clause() const {
        return _raw.where_clause();
    }

    cql3::statements::select_statement& select_statement() const;
    const query::partition_slice& partition_slice() const;
    const dht::partition_range_vector& partition_ranges() const;
    const column_definition* view_column(const schema& base, column_id base_id) const;
    stdx::optional<column_id> base_non_pk_column_in_view_pk(const schema& base) const;

    friend bool operator==(const view_info& x, const view_info& y) {
        return x._raw == y._raw;
    }
    friend std::ostream& operator<<(std::ostream& os, const view_info& view) {
        return os << view._raw;
    }
};
