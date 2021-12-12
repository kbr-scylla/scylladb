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

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/relation.hh"
#include "cql3/attributes.hh"
#include "db/config.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {

namespace selection {
    class selection;
    class raw_selector;
} // namespace selection

namespace restrictions {
    class statement_restrictions;
} // namespace restrictions

namespace statements {

namespace raw {

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
class select_statement : public cf_statement
{
public:
    // Ordering of selected values as defined by the basic comparison order.
    // Even for a column that by default has ordering 4, 3, 2, 1 ordering it in ascending order will result in 1, 2, 3, 4.
    enum class ordering {
        ascending,
        descending
    };
    class parameters final {
    public:
        using orderings_type = std::vector<std::pair<shared_ptr<column_identifier::raw>, ordering>>;
    private:
        const orderings_type _orderings;
        const bool _is_distinct;
        const bool _allow_filtering;
        const bool _is_json;
        bool _bypass_cache = false;
    public:
        parameters();
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering);
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering,
            bool is_json,
            bool bypass_cache);
        bool is_distinct() const;
        bool allow_filtering() const;
        bool is_json() const;
        bool bypass_cache() const;
        orderings_type const& orderings() const;
    };
    template<typename T>
    using compare_fn = std::function<bool(const T&, const T&)>;

    using result_row_type = std::vector<bytes_opt>;
    using ordering_comparator_type = compare_fn<result_row_type>;
protected:
    virtual audit::statement_category category() const override;
private:
    using prepared_orderings_type = std::vector<std::pair<const column_definition*, ordering>>;
private:
    lw_shared_ptr<const parameters> _parameters;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    std::vector<::shared_ptr<relation>> _where_clause;
    std::optional<expr::expression> _limit;
    std::optional<expr::expression> _per_partition_limit;
    std::vector<::shared_ptr<cql3::column_identifier::raw>> _group_by_columns;
    std::unique_ptr<cql3::attributes::raw> _attrs;
public:
    select_statement(cf_name cf_name,
            lw_shared_ptr<const parameters> parameters,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            std::vector<::shared_ptr<relation>> where_clause,
            std::optional<expr::expression> limit,
            std::optional<expr::expression> per_partition_limit,
            std::vector<::shared_ptr<cql3::column_identifier::raw>> group_by_columns,
            std::unique_ptr<cql3::attributes::raw> attrs);

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override {
        return prepare(db, stats, false);
    }
    std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats, bool for_view);
private:
    void maybe_jsonize_select_clause(database& db, schema_ptr schema);
    ::shared_ptr<restrictions::statement_restrictions> prepare_restrictions(
        database& db,
        schema_ptr schema,
        prepare_context& ctx,
        ::shared_ptr<selection::selection> selection,
        bool for_view = false,
        bool allow_filtering = false);

    /** Returns an expression for the limit or nullopt if no limit is set */
    std::optional<expr::expression> prepare_limit(database& db, prepare_context& ctx, const std::optional<expr::expression>& limit);

    // Checks whether it is legal to have ORDER BY in this statement
    static void verify_ordering_is_allowed(const restrictions::statement_restrictions& restrictions);

    void handle_unrecognized_ordering_column(const column_identifier& column) const;

    // Processes ORDER BY column orderings, converts column_identifiers to column_defintions
    prepared_orderings_type prepare_orderings(const schema& schema) const;

    void verify_ordering_is_valid(const prepared_orderings_type&, const schema&, const restrictions::statement_restrictions& restrictions) const;

    // Checks whether this ordering reverses all results.
    // We only allow leaving select results unchanged or reversing them.
    bool is_ordering_reversed(const prepared_orderings_type&) const;

    select_statement::ordering_comparator_type get_ordering_comparator(
        const prepared_orderings_type&,
        selection::selection& selection,
        const restrictions::statement_restrictions& restrictions);

    static void validate_distinct_selection(const schema& schema,
        const selection::selection& selection,
        const restrictions::statement_restrictions& restrictions);

    /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
    void check_needs_filtering(
            const restrictions::statement_restrictions& restrictions,
            db::tri_mode_restriction_t::mode strict_allow_filtering,
            std::vector<sstring>& warnings);

    void ensure_filtering_columns_retrieval(database& db,
                                            selection::selection& selection,
                                            const restrictions::statement_restrictions& restrictions);

    /// Returns indices of GROUP BY cells in fetched rows.
    std::vector<size_t> prepare_group_by(const schema& schema, selection::selection& selection) const;

    bool contains_alias(const column_identifier& name) const;

    lw_shared_ptr<column_specification> limit_receiver(bool per_partition = false);

#if 0
    public:
        virtual sstring to_string() override {
            return sstring("raw_statement(")
                + "name=" + cf_name->to_string()
                + ", selectClause=" + to_string(_select_clause)
                + ", whereClause=" + to_string(_where_clause)
                + ", isDistinct=" + to_string(_parameters->is_distinct())
                + ", isJson=" + to_string(_parameters->is_json())
                + ")";
        }
    };
#endif
};

}

}

}
