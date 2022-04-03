/*
 */

/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "schema.hh"

#include "data_dictionary/data_dictionary.hh"

#include <vector>
#include <set>

namespace cql3::expr {

enum class oper_t;

}

namespace secondary_index {

sstring index_table_name(const sstring& index_name);

/*!
 * \brief a reverse of index_table_name
 * It gets a table_name and return the index name that was used
 * to create that table.
 */
sstring index_name_from_table_name(const sstring& table_name);

class index {
    sstring _target_column;
    index_metadata _im;
public:
    index(const sstring& target_column, const index_metadata& im);
    bool depends_on(const column_definition& cdef) const;
    bool supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const;
    const index_metadata& metadata() const;
    const sstring& target_column() const {
        return _target_column;
    }
};

class secondary_index_manager {
    data_dictionary::table _cf;
    /// The key of the map is the name of the index as stored in system tables.
    std::unordered_map<sstring, index> _indices;
public:
    secondary_index_manager(data_dictionary::table cf);
    void reload();
    view_ptr create_view_for_index(const index_metadata& index, bool new_token_column_computation) const;
    std::vector<index_metadata> get_dependent_indices(const column_definition& cdef) const;
    std::vector<index> list_indexes() const;
    bool is_index(view_ptr) const;
    bool is_index(const schema& s) const;
    bool is_global_index(const schema& s) const;
private:
    void add_index(const index_metadata& im);
};

}
