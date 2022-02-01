/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "schema_fwd.hh"
#include "column_identifier.hh"
#include "prepare_context.hh"
#include "restrictions/restriction.hh"
#include "statements/bound.hh"
#include "expr/expression.hh"

namespace cql3 {

class relation : public enable_shared_from_this<relation> {
protected:
    expr::oper_t _relation_type;
public:
    relation(const expr::oper_t& relation_type)
        : _relation_type(relation_type) {
    }
    virtual ~relation() {}

    virtual const expr::oper_t& get_operator() const {
        return _relation_type;
    }

    /**
     * Checks if this relation apply to multiple columns.
     *
     * @return <code>true</code> if this relation apply to multiple columns, <code>false</code> otherwise.
     */
    virtual bool is_multi_column() const {
        return false;
    }

    /**
     * Checks if this relation is a token relation (e.g. <pre>token(a) = token(1)</pre>).
     *
     * @return <code>true</code> if this relation is a token relation, <code>false</code> otherwise.
     */
    virtual bool on_token() const {
        return false;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_contains() const final {
        return _relation_type == expr::oper_t::CONTAINS;
    }

    /**
     * Checks if the operator of this relation is a <code>CONTAINS_KEY</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>CONTAINS_KEY</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_contains_key() const final {
        return _relation_type == expr::oper_t::CONTAINS_KEY;
    }

    /**
     * Checks if the operator of this relation is a <code>IN</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>IN</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_IN() const final {
        return _relation_type == expr::oper_t::IN;
    }

    /**
     * Checks if the operator of this relation is a <code>EQ</code>.
     * @return <code>true</code>  if the operator of this relation is a <code>EQ</code>, <code>false</code>
     * otherwise.
     */
    virtual bool is_EQ() const final {
        return _relation_type == expr::oper_t::EQ;
    }

    /**
     * Checks if the operator of this relation is a <code>Slice</code> (GT, GTE, LTE, LT).
     *
     * @return <code>true</code> if the operator of this relation is a <code>Slice</code>, <code>false</code> otherwise.
     */
    virtual bool is_slice() const final {
        return _relation_type == expr::oper_t::GT
                || _relation_type == expr::oper_t::GTE
                || _relation_type == expr::oper_t::LTE
                || _relation_type ==
            expr::oper_t::LT;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param cfm the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> to_restriction(data_dictionary::database db, schema_ptr schema, prepare_context& ctx) final {
        if (_relation_type == expr::oper_t::EQ) {
            return new_EQ_restriction(db, schema, ctx);
        } else if (_relation_type == expr::oper_t::LT) {
            return new_slice_restriction(db, schema, ctx, statements::bound::END, false);
        } else if (_relation_type == expr::oper_t::LTE) {
            return new_slice_restriction(db, schema, ctx, statements::bound::END, true);
        } else if (_relation_type == expr::oper_t::GTE) {
            return new_slice_restriction(db, schema, ctx, statements::bound::START, true);
        } else if (_relation_type == expr::oper_t::GT) {
            return new_slice_restriction(db, schema, ctx, statements::bound::START, false);
        } else if (_relation_type == expr::oper_t::IN) {
            return new_IN_restriction(db, schema, ctx);
        } else if (_relation_type == expr::oper_t::CONTAINS) {
            return new_contains_restriction(db, schema, ctx, false);
        } else if (_relation_type == expr::oper_t::CONTAINS_KEY) {
            return new_contains_restriction(db, schema, ctx, true);
        } else if (_relation_type == expr::oper_t::IS_NOT) {
            // This case is not supposed to happen: statement_restrictions
            // constructor does not call this function for views' IS_NOT.
            throw exceptions::invalid_request_exception(format("Unsupported \"IS NOT\" relation: {}", to_string()));
        } else if (_relation_type == expr::oper_t::LIKE) {
            return new_LIKE_restriction(db, schema, ctx);
        } else {
            throw exceptions::invalid_request_exception(format("Unsupported \"!=\" relation: {}", to_string()));
        }
    }

    virtual sstring to_string() const = 0;

    friend std::ostream& operator<<(std::ostream& out, const relation& r) {
        return out << r.to_string();
    }

    /**
     * Creates a new EQ restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param boundNames the variables specification where to collect the bind variables
     * @return a new EQ restriction instance.
     * @throws InvalidRequestException if the relation cannot be converted into an EQ restriction.
     */
    virtual ::shared_ptr<restrictions::restriction> new_EQ_restriction(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx) = 0;

    /**
     * Creates a new IN restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param meta the variables specification where to collect the bind variables
     * @return a new IN restriction instance
     * @throws InvalidRequestException if the relation cannot be converted into an IN restriction.
     */
    virtual ::shared_ptr<restrictions::restriction> new_IN_restriction(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx) = 0;

    /**
     * Creates a new Slice restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param meta the variables specification where to collect the bind variables
     * @param bound the slice bound
     * @param inclusive <code>true</code> if the bound is included.
     * @return a new slice restriction instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> new_slice_restriction(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx,
        statements::bound bound,
        bool inclusive) = 0;

    /**
     * Creates a new Contains restriction instance.
     *
     * @param cfm the Column Family meta data
     * @param meta the variables specification where to collect the bind variables
     * @param isKey <code>true</code> if the restriction to create is a CONTAINS KEY
     * @return a new Contains <code>::shared_ptr<restrictions::restriction></code> instance
     * @throws InvalidRequestException if the <code>Relation</code> is not valid
     */
    virtual ::shared_ptr<restrictions::restriction> new_contains_restriction(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx, bool isKey) = 0;

    /**
     * Creates a new LIKE restriction instance.
     */
    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx) = 0;

    /**
     * Renames an identifier in this Relation, if applicable.
     * @param from the old identifier
     * @param to the new identifier
     * @return a pointer object, if the old identifier is not in the set of entities that this relation covers;
     *         otherwise a new Relation with "from" replaced by "to" is returned.
     */
    virtual ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) = 0;

protected:

    /**
     * Converts the specified <code>Raw</code> into an <code>Expression</code>.
     * @param receivers the columns to which the values must be associated at
     * @param raw the raw expression to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Expression</code> corresponding to the specified <code>Raw</code>
     * @throws InvalidRequestException if the <code>Raw</code> expression is not valid
     */
    virtual expr::expression to_expression(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                           const expr::expression& raw,
                                           data_dictionary::database db,
                                           const sstring& keyspace,
                                           prepare_context& ctx) const = 0;

    /**
     * Converts the specified <code>Raw</code> expressions into <code>expressions</code>s.
     * @param receivers the columns to which the values must be associated at
     * @param raws the raw expressions to convert
     * @param keyspace the keyspace name
     * @param boundNames the variables specification where to collect the bind variables
     *
     * @return the <code>Expression</code>s corresponding to the specified <code>Raw</code> expressions
     * @throws InvalidRequestException if the <code>Raw</code> expressions are not valid
     */
    std::vector<expr::expression> to_expressions(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                                 const std::vector<expr::expression>& raws,
                                                 data_dictionary::database db,
                                                 const sstring& keyspace,
                                                 prepare_context& ctx) const {
        std::vector<expr::expression> expressions;
        expressions.reserve(raws.size());
        for (const auto& r : raws) {
            expressions.emplace_back(to_expression(receivers, r, db, keyspace, ctx));
        }
        return expressions;
    }

    /**
     * Converts the specified entity into a column definition.
     *
     * @param cfm the column family meta data
     * @param entity the entity to convert
     * @return the column definition corresponding to the specified entity
     * @throws InvalidRequestException if the entity cannot be recognized
     */
    virtual const column_definition& to_column_definition(const schema& schema, const column_identifier::raw& entity) final;
};

using relation_ptr = ::shared_ptr<relation>;

}
