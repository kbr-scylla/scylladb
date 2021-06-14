/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/sstring.hh>

#include "cql3/column_identifier.hh"
#include "cql3/CqlParser.hpp"
#include "cql3/error_collector.hh"
#include "cql3/relation.hh"
#include "cql3/statements/raw/select_statement.hh"

namespace cql3 {

namespace util {


void do_with_parser_impl(const sstring_view& cql, noncopyable_function<void (cql3_parser::CqlParser& p)> func);

template <typename Func, typename Result = std::result_of_t<Func(cql3_parser::CqlParser&)>>
Result do_with_parser(const sstring_view& cql, Func&& f) {
    std::optional<Result> ret;
    do_with_parser_impl(cql, [&] (cql3_parser::CqlParser& parser) {
        ret.emplace(f(parser));
    });
    return std::move(*ret);
}

template<typename Range> // Range<cql3::relation_ptr>
sstring relations_to_where_clause(Range&& relations) {
    auto expressions = relations | boost::adaptors::transformed(std::mem_fn(&relation::to_string));
    return boost::algorithm::join(expressions, " AND ");
}

static std::vector<relation_ptr> where_clause_to_relations(const sstring_view& where_clause) {
    return do_with_parser(where_clause, std::mem_fn(&cql3_parser::CqlParser::whereClause));
}

inline sstring rename_column_in_where_clause(const sstring_view& where_clause, column_identifier::raw from, column_identifier::raw to) {
    auto relations = where_clause_to_relations(where_clause);
    auto new_relations = relations | boost::adaptors::transformed([&] (auto&& rel) {
        return rel->maybe_rename_identifier(from, to);
    });
    return relations_to_where_clause(std::move(new_relations));
}

/// build a CQL "select" statement with the desired parameters.
/// If select_all_columns==true, all columns are selected and the value of
/// selected_columns is ignored.
std::unique_ptr<cql3::statements::raw::select_statement> build_select_statement(
        const sstring_view& cf_name,
        const sstring_view& where_clause,
        bool select_all_columns,
        const std::vector<column_definition>& selected_columns);

/// maybe_quote() takes an identifier - the name of a column, table or
/// keyspace name - and transforms it to a string which can be used in CQL
/// commands. Namely, if the identifier is not entirely lower-case (including
/// digits and underscores), it needs to be quoted to be represented in CQL.
/// Without this quoting, CQL folds uppercase letters to lower case, and
/// forbids non-alpha-numeric characters in identifier names.
/// Quoting involves wrapping the string in double-quotes ("). A double-quote
/// character itself is quoted by doubling it.
sstring maybe_quote(const sstring& s);

// Check whether timestamp is not too far in the future as this probably
// indicates its incorrectness (for example using other units than microseconds).
void validate_timestamp(const query_options& options, const std::unique_ptr<attributes>& attrs);

} // namespace util

} // namespace cql3
