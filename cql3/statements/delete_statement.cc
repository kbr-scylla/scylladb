/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/range/adaptors.hpp>

#include "cql3/tuples.hh"
#include "database.hh"
#include "delete_statement.hh"
#include "raw/delete_statement.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {

namespace statements {

delete_statement::delete_statement(audit::audit_info_ptr&& audit_info, statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats)
        : modification_statement{type, bound_terms, std::move(s), std::move(attrs), stats}
{
    set_audit_info(std::move(audit_info));
}

bool delete_statement::require_full_clustering_key() const {
    return false;
}

bool delete_statement::allow_clustering_key_slices() const {
    return true;
}

void delete_statement::add_update_for_key(mutation& m, const query::clustering_range& range, const update_parameters& params, const json_cache_opt& json_cache) const {
    if (_column_operations.empty()) {
        if (s->clustering_key_size() == 0 || range.is_full()) {
            m.partition().apply(params.make_tombstone());
        } else if (range.is_singular()) {
            m.partition().apply_delete(*s, range.start()->value(), params.make_tombstone());
        } else {
            auto bvs = bound_view::from_range(range);
            m.partition().apply_delete(*s, range_tombstone(bvs.first, bvs.second, params.make_tombstone()));
        }
        return;
    }

    for (auto&& op : _column_operations) {
        op->execute(m, range.start() ? std::move(range.start()->value()) : clustering_key_prefix::make_empty(), params);
    }
}

namespace raw {

::shared_ptr<cql3::statements::modification_statement>
delete_statement::prepare_internal(database& db, schema_ptr schema, variable_specifications& bound_names,
        std::unique_ptr<attributes> attrs, cql_stats& stats) const {
    auto stmt = ::make_shared<cql3::statements::delete_statement>(audit_info(), statement_type::DELETE, bound_names.size(), schema, std::move(attrs), stats);

    for (auto&& deletion : _deletions) {
        auto&& id = deletion->affected_column().prepare_column_identifier(*schema);
        auto def = get_column_definition(*schema, *id);
        if (!def) {
            throw exceptions::invalid_request_exception(format("Unknown identifier {}", *id));
        }

        // For compact, we only have one value except the key, so the only form of DELETE that make sense is without a column
        // list. However, we support having the value name for coherence with the static/sparse case
        if (def->is_primary_key()) {
            throw exceptions::invalid_request_exception(format("Invalid identifier {} for deletion (should not be a PRIMARY KEY part)", def->name_as_text()));
        }

        auto&& op = deletion->prepare(db, schema->ks_name(), *def);
        op->collect_marker_specification(bound_names);
        stmt->add_operation(op);
    }
    prepare_conditions(db, *schema, bound_names, *stmt);
    stmt->process_where_clause(db, _where_clause, bound_names);
    if (has_slice(stmt->restrictions().get_clustering_columns_restrictions()->expression)) {
        if (!schema->is_compound()) {
            throw exceptions::invalid_request_exception("Range deletions on \"compact storage\" schemas are not supported");
        }
        if (!_deletions.empty()) {
            throw exceptions::invalid_request_exception("Range deletions are not supported for specific columns");
        }
    }
    return stmt;
}

delete_statement::delete_statement(cf_name name,
                                 std::unique_ptr<attributes::raw> attrs,
                                 std::vector<std::unique_ptr<operation::raw_deletion>> deletions,
                                 std::vector<::shared_ptr<relation>> where_clause,
                                 conditions_vector conditions,
                                 bool if_exists)
    : raw::modification_statement(std::move(name), std::move(attrs), std::move(conditions), false, if_exists)
    , _deletions(std::move(deletions))
    , _where_clause(std::move(where_clause))
{
    if (_attrs->time_to_live) {
        throw exceptions::invalid_request_exception("TTL attribute is not allowed for deletes");
    }
}

}

}

}
