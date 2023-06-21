/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <fmt/core.h>
#include <ostream>
#include <seastar/core/shared_ptr.hh>
#include <variant>
#include <concepts>

#include "cql3/column_identifier.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function_name.hh"
#include "seastarx.hh"
#include "utils/overloaded_functor.hh"
#include "utils/variant_element.hh"
#include "cql3/values.hh"

class row;

namespace db {
namespace functions {
    class function;
}
}

namespace secondary_index {
class index;
class secondary_index_manager;
} // namespace secondary_index

namespace query {
    class result_row_view;
} // namespace query

namespace cql3 {
struct prepare_context;

class column_identifier_raw;
class query_options;

namespace selection {
    class selection;
} // namespace selection

namespace restrictions {
    class restriction;
}

namespace expr {

struct allow_local_index_tag {};
using allow_local_index = bool_class<allow_local_index_tag>;

struct binary_operator;
struct conjunction;
struct column_value;
struct subscript;
struct unresolved_identifier;
struct column_mutation_attribute;
struct function_call;
struct cast;
struct field_selection;
struct bind_variable;
struct untyped_constant;
struct constant;
struct tuple_constructor;
struct collection_constructor;
struct usertype_constructor;

template <typename T>
concept ExpressionElement
        = std::same_as<T, conjunction>
        || std::same_as<T, binary_operator>
        || std::same_as<T, column_value>
        || std::same_as<T, subscript>
        || std::same_as<T, unresolved_identifier>
        || std::same_as<T, column_mutation_attribute>
        || std::same_as<T, function_call>
        || std::same_as<T, cast>
        || std::same_as<T, field_selection>
        || std::same_as<T, bind_variable>
        || std::same_as<T, untyped_constant>
        || std::same_as<T, constant>
        || std::same_as<T, tuple_constructor>
        || std::same_as<T, collection_constructor>
        || std::same_as<T, usertype_constructor>
        ;

template <typename Func>
concept invocable_on_expression
        = std::invocable<Func, conjunction>
        && std::invocable<Func, binary_operator>
        && std::invocable<Func, column_value>
        && std::invocable<Func, subscript>
        && std::invocable<Func, unresolved_identifier>
        && std::invocable<Func, column_mutation_attribute>
        && std::invocable<Func, function_call>
        && std::invocable<Func, cast>
        && std::invocable<Func, field_selection>
        && std::invocable<Func, bind_variable>
        && std::invocable<Func, untyped_constant>
        && std::invocable<Func, constant>
        && std::invocable<Func, tuple_constructor>
        && std::invocable<Func, collection_constructor>
        && std::invocable<Func, usertype_constructor>
        ;

template <typename Func>
concept invocable_on_expression_ref
        = std::invocable<Func, conjunction&>
        && std::invocable<Func, binary_operator&>
        && std::invocable<Func, column_value&>
        && std::invocable<Func, subscript&>
        && std::invocable<Func, unresolved_identifier&>
        && std::invocable<Func, column_mutation_attribute&>
        && std::invocable<Func, function_call&>
        && std::invocable<Func, cast&>
        && std::invocable<Func, field_selection&>
        && std::invocable<Func, bind_variable&>
        && std::invocable<Func, untyped_constant&>
        && std::invocable<Func, constant&>
        && std::invocable<Func, tuple_constructor&>
        && std::invocable<Func, collection_constructor&>
        && std::invocable<Func, usertype_constructor&>
        ;

/// A CQL expression -- union of all possible expression types.
class expression final {
    // 'impl' holds a variant of all expression types, but since 
    // variants of incomplete types are not allowed, we forward declare it
    // here and fully define it later.
    struct impl;                 
    std::unique_ptr<impl> _v;
public:
    expression(); // FIXME: remove
    expression(ExpressionElement auto e);

    expression(const expression&);
    expression(expression&&) noexcept = default;
    expression& operator=(const expression&);
    expression& operator=(expression&&) noexcept = default;

    template <invocable_on_expression Visitor>
    friend decltype(auto) visit(Visitor&& visitor, const expression& e);

    template <invocable_on_expression_ref Visitor>
    friend decltype(auto) visit(Visitor&& visitor, expression& e);

    template <ExpressionElement E>
    friend bool is(const expression& e);

    template <ExpressionElement E>
    friend const E& as(const expression& e);

    template <ExpressionElement E>
    friend const E* as_if(const expression* e);

    template <ExpressionElement E>
    friend E* as_if(expression* e);

    // Prints given expression using additional options
    struct printer {
        const expression& expr_to_print;
        bool debug_mode = true;
    };

    friend bool operator==(const expression& e1, const expression& e2);
};

/// Checks if two expressions are equal. If they are, they definitely
/// perform the same computation. If they are unequal, they may perform
/// the same computation or different computations.
bool operator==(const expression& e1, const expression& e2);

// An expression that doesn't contain subexpressions
template <typename E>
concept LeafExpression
        = std::same_as<unresolved_identifier, E>
        || std::same_as<bind_variable, E> 
        || std::same_as<untyped_constant, E> 
        || std::same_as<constant, E>
        || std::same_as<column_value, E>
        ;

/// A column, usually encountered on the left side of a restriction.
/// An expression like `mycol < 5` would be expressed as a binary_operator
/// with column_value on the left hand side.
struct column_value {
    const column_definition* col;

    column_value(const column_definition* col) : col(col) {}

    friend bool operator==(const column_value&, const column_value&) = default;
};

/// A subscripted value, eg list_colum[2], val[sub]
struct subscript {
    expression val;
    expression sub;
    data_type type; // may be null before prepare

    friend bool operator==(const subscript&, const subscript&) = default;
};

/// Gets the subscripted column_value out of the subscript.
/// Only columns can be subscripted in CQL, so we can expect that the subscripted expression is a column_value.
const column_value& get_subscripted_column(const subscript&);

/// Gets the column_definition* out of expression that can be a column_value or subscript
/// Only columns can be subscripted in CQL, so we can expect that the subscripted expression is a column_value.
const column_value& get_subscripted_column(const expression&);

enum class oper_t { EQ, NEQ, LT, LTE, GTE, GT, IN, CONTAINS, CONTAINS_KEY, IS_NOT, LIKE };

/// Describes the nature of clustering-key comparisons.  Useful for implementing SCYLLA_CLUSTERING_BOUND.
enum class comparison_order : char {
    cql, ///< CQL order. (a,b)>(1,1) is equivalent to a>1 OR (a=1 AND b>1).
    clustering, ///< Table's clustering order. (a,b)>(1,1) means any row past (1,1) in storage.
};

enum class null_handling_style {
    sql,           // evaluate(NULL = NULL) -> NULL, evaluate(NULL < x) -> NULL
    lwt_nulls,     // evaluate(NULL = NULL) -> TRUE, evaluate(NULL < x) -> exception
};

/// Operator restriction: LHS op RHS.
struct binary_operator {
    expression lhs;
    oper_t op;
    expression rhs;
    comparison_order order;
    null_handling_style null_handling = null_handling_style::sql;

    binary_operator(expression lhs, oper_t op, expression rhs, comparison_order order = comparison_order::cql);

    friend bool operator==(const binary_operator&, const binary_operator&) = default;
};

/// A conjunction of restrictions.
struct conjunction {
    std::vector<expression> children;

    friend bool operator==(const conjunction&, const conjunction&) = default;
};

// Gets resolved eventually into a column_value.
struct unresolved_identifier {
    ::shared_ptr<column_identifier_raw> ident;

    ~unresolved_identifier();

    friend bool operator==(const unresolved_identifier&, const unresolved_identifier&) = default;
};

// An attribute attached to a column mutation: writetime or ttl
struct column_mutation_attribute {
    enum class attribute_kind { writetime, ttl };

    attribute_kind kind;
    // note: only unresolved_identifier is legal here now. One day, when prepare()
    // on expressions yields expressions, column_value will also be legal here.
    expression column;

    friend bool operator==(const column_mutation_attribute&, const column_mutation_attribute&) = default;
};

struct function_call {
    std::variant<functions::function_name, shared_ptr<db::functions::function>> func;
    std::vector<expression> args;

    // 0-based index of the function call within a CQL statement.
    // Used to populate the cache of execution results while passing to
    // another shard (handling `bounce_to_shard` messages) in LWT statements.
    //
    // The id is set only for the function calls that are a part of LWT
    // statement restrictions for the partition key. Otherwise, the id is not
    // set and the call is not considered when using or populating the cache.
    //
    // For example in a query like:
    // INSERT INTO t (pk) VALUES (uuid()) IF NOT EXISTS
    // The query should be executed on a shard that has the pk partition,
    // but it changes with each uuid() call.
    // uuid() call result is cached and sent to the proper shard.
    //
    // Cache id is kept in shared_ptr because of how prepare_context works.
    // During fill_prepare_context all function cache ids are collected
    // inside prepare_context.
    // Later when some condition occurs we might decide to clear
    // cache ids of all function calls found in prepare_context.
    // However by this time these function calls could have been
    // copied multiple times. Prepare_context keeps a shared_ptr
    // to function_call ids, and then clearing the shared id
    // clears it in all possible copies.
    // This logic was introduced back when everything was shared_ptr<term>,
    // now a better solution might exist.
    //
    // This field can be nullptr, it means that there is no cache id set.
    ::shared_ptr<std::optional<uint8_t>> lwt_cache_id;

    friend bool operator==(const function_call&, const function_call&) = default;
};

struct cast {
    enum class cast_style { c, sql };
    cast_style style;
    expression arg;
    std::variant<data_type, shared_ptr<cql3_type::raw>> type;

    friend bool operator==(const cast&, const cast&) = default;
};

struct field_selection {
    expression structure;
    shared_ptr<column_identifier_raw> field;
    size_t field_idx = 0; // invalid before prepare
    data_type type; // may be null before prepare

    friend bool operator==(const field_selection&, const field_selection&) = default;
};

struct bind_variable {
    int32_t bind_index;

    // Describes where this bound value will be assigned.
    // Contains value type and other useful information.
    ::lw_shared_ptr<column_specification> receiver;

    friend bool operator==(const bind_variable&, const bind_variable&) = default;
};

// A constant which does not yet have a date type. It is partially typed
// (we know if it's floating or int) but not sized.
struct untyped_constant {
    enum type_class { integer, floating_point, string, boolean, duration, uuid, hex, null };
    type_class partial_type;
    sstring raw_text;

    friend bool operator==(const untyped_constant&, const untyped_constant&) = default;
};

untyped_constant make_untyped_null();

// Represents a constant value with known value and type
// For null and unset the type can sometimes be set to empty_type
struct constant {
    cql3::raw_value value;

    // Never nullptr, for NULL and UNSET might be empty_type
    data_type type;

    constant(cql3::raw_value value, data_type type);
    static constant make_null(data_type val_type = empty_type);
    static constant make_bool(bool bool_val);

    bool is_null() const;
    bool is_unset_value() const;
    bool is_null_or_unset() const;
    bool has_empty_value_bytes() const;

    cql3::raw_value_view view() const;

    friend bool operator==(const constant&, const constant&) = default;
};

// Denotes construction of a tuple from its elements, e.g.  ('a', ?, some_column) in CQL.
struct tuple_constructor {
    std::vector<expression> elements;

    // Might be nullptr before prepare.
    // After prepare always holds a valid type, although it might be reversed_type(tuple_type).
    data_type type;

    friend bool operator==(const tuple_constructor&, const tuple_constructor&) = default;
};

// Constructs a collection of same-typed elements
struct collection_constructor {
    enum class style_type { list, set, map };
    style_type style;
    std::vector<expression> elements;

    // Might be nullptr before prepare.
    // After prepare always holds a valid type, although it might be reversed_type(collection_type).
    data_type type;

    friend bool operator==(const collection_constructor&, const collection_constructor&) = default;
};

// Constructs an object of a user-defined type
struct usertype_constructor {
    using elements_map_type = std::unordered_map<column_identifier, expression>;
    elements_map_type elements;

    // Might be nullptr before prepare.
    // After prepare always holds a valid type, although it might be reversed_type(user_type).
    data_type type;

    friend bool operator==(const usertype_constructor&, const usertype_constructor&) = default;
};

// now that all expression types are fully defined, we can define expression::impl
struct expression::impl final {
    using variant_type = std::variant<
            conjunction, binary_operator, column_value, unresolved_identifier,
            column_mutation_attribute, function_call, cast, field_selection,
            bind_variable, untyped_constant, constant, tuple_constructor, collection_constructor,
            usertype_constructor, subscript>;
    variant_type v;
    impl(variant_type v) : v(std::move(v)) {}
};

expression::expression(ExpressionElement auto e)
        : _v(std::make_unique<impl>(std::move(e))) {
}

inline expression::expression()
        : expression(conjunction{}) {
}

template <invocable_on_expression Visitor>
decltype(auto) visit(Visitor&& visitor, const expression& e) {
    return std::visit(std::forward<Visitor>(visitor), e._v->v);
}

template <invocable_on_expression_ref Visitor>
decltype(auto) visit(Visitor&& visitor, expression& e) {
    return std::visit(std::forward<Visitor>(visitor), e._v->v);
}

template <ExpressionElement E>
bool is(const expression& e) {
    return std::holds_alternative<E>(e._v->v);
}

template <ExpressionElement E>
const E& as(const expression& e) {
    return std::get<E>(e._v->v);
}

template <ExpressionElement E>
const E* as_if(const expression* e) {
    return std::get_if<E>(&e->_v->v);
}

template <ExpressionElement E>
E* as_if(expression* e) {
    return std::get_if<E>(&e->_v->v);
}

/// Creates a conjunction of a and b.  If either a or b is itself a conjunction, its children are inserted
/// directly into the resulting conjunction's children, flattening the expression tree.
extern expression make_conjunction(expression a, expression b);

extern std::ostream& operator<<(std::ostream&, oper_t);

extern sstring to_string(const expression&);

extern std::ostream& operator<<(std::ostream&, const column_value&);

extern std::ostream& operator<<(std::ostream&, const expression&);

extern std::ostream& operator<<(std::ostream&, const expression::printer&);

data_type type_of(const expression& e);


} // namespace expr

} // namespace cql3

/// Custom formatter for an expression. Use {:user} for user-oriented
/// output, {:debug} for debug-oriented output. User is the default.
///
/// Required for fmt::join() to work on expression.
template <>
class fmt::formatter<cql3::expr::expression> {
    bool _debug = false;
private:
    constexpr static bool try_match_and_advance(format_parse_context& ctx, std::string_view s) {
        auto [ctx_end, s_end] = std::ranges::mismatch(ctx, s);
        if (s_end == s.end()) {
            ctx.advance_to(ctx_end);
            return true;
        }
        return false;
    }
public:
    constexpr auto parse(format_parse_context& ctx) {
        using namespace std::string_view_literals;
        if (try_match_and_advance(ctx, "debug"sv)) {
            _debug = true;
        } else if (try_match_and_advance(ctx, "user"sv)) {
            _debug = false;
        }
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::expression& expr, FormatContext& ctx) const {
        std::ostringstream os;
        os << cql3::expr::expression::printer{.expr_to_print = expr, .debug_mode = _debug};
        return fmt::format_to(ctx.out(), "{}", os.str());
    }
};

/// Required for fmt::join() to work on expression::printer.
template <>
struct fmt::formatter<cql3::expr::expression::printer> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::expression::printer& pr, FormatContext& ctx) const {
        std::ostringstream os;
        os << pr;
        return fmt::format_to(ctx.out(), "{}", os.str());
    }
};

/// Required for fmt::join() to work on ExpressionElement, and for {:user}/{:debug} to work on ExpressionElement.
template <cql3::expr::ExpressionElement E>
struct fmt::formatter<E> : public fmt::formatter<cql3::expr::expression> {
};

template <>
struct fmt::formatter<cql3::expr::column_mutation_attribute::attribute_kind> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(cql3::expr::column_mutation_attribute::attribute_kind k, FormatContext& ctx) const {
        switch (k) {
            case cql3::expr::column_mutation_attribute::attribute_kind::writetime:
                return fmt::format_to(ctx.out(), "WRITETIME");
            case cql3::expr::column_mutation_attribute::attribute_kind::ttl:
                return fmt::format_to(ctx.out(), "TTL");
        }
        return fmt::format_to(ctx.out(), "unrecognized_attribute_kind({})", static_cast<int>(k));
    }
};
