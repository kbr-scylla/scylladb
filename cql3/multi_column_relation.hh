/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/relation.hh"
#include "cql3/restrictions/multi_column_restriction.hh"

#include <ranges>

namespace cql3 {

/**
 * A relation using the tuple notation, which typically affects multiple columns.
 * Examples:
 *  - SELECT ... WHERE (a, b, c) > (1, 'a', 10)
 *  - SELECT ... WHERE (a, b, c) IN ((1, 2, 3), (4, 5, 6))
 *  - SELECT ... WHERE (a, b) < ?
 *  - SELECT ... WHERE (a, b) IN ?
 */
class multi_column_relation final : public relation {
public:
    using mode = expr::comparison_order;
private:
    std::vector<shared_ptr<column_identifier::raw>> _entities;
    std::optional<expr::expression> _values_or_marker;
    std::vector<expr::expression> _in_values;
    std::optional<expr::expression> _in_marker;
    mode _mode;
public:
    multi_column_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
        expr::oper_t relation_type, std::optional<expr::expression> values_or_marker,
        std::vector<expr::expression> in_values, std::optional<expr::expression> in_marker, mode m = mode::cql)
        : relation(relation_type)
        , _entities(std::move(entities))
        , _values_or_marker(std::move(values_or_marker))
        , _in_values(std::move(in_values))
        , _in_marker(std::move(in_marker))
        , _mode(m)
    { }

    static shared_ptr<multi_column_relation> create_multi_column_relation(
        std::vector<shared_ptr<column_identifier::raw>> entities, expr::oper_t relation_type,
        std::optional<expr::expression> values_or_marker, std::vector<expr::expression> in_values,
        std::optional<expr::expression> in_marker, mode m = mode::cql) {
        return ::make_shared<multi_column_relation>(std::move(entities), relation_type, std::move(values_or_marker),
            std::move(in_values), std::move(in_marker), m);
    }

    /**
     * Creates a multi-column EQ, LT, LTE, GT, or GTE relation.
     * For example: "SELECT ... WHERE (a, b) > (0, 1)"
     * @param entities the columns on the LHS of the relation
     * @param relationType the relation operator
     * @param valuesOrMarker a Tuples.Literal instance or a Tuples.Raw marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_non_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                    expr::oper_t relation_type, expr::expression values_or_marker) {
        assert(relation_type != expr::oper_t::IN);
        return create_multi_column_relation(std::move(entities), relation_type, std::move(values_or_marker), {}, {});
    }

    /**
     * Same as above, but sets the magic mode that causes us to treat the restrictions as "raw" clustering bounds
     */
    static shared_ptr<multi_column_relation> create_scylla_clustering_bound_non_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                    expr::oper_t relation_type, expr::expression values_or_marker) {
        assert(relation_type != expr::oper_t::IN);
        return create_multi_column_relation(std::move(entities), relation_type, std::move(values_or_marker), {}, {}, mode::clustering);
    }

    /**
     * Creates a multi-column IN relation with a list of IN values or markers.
     * For example: "SELECT ... WHERE (a, b) IN ((0, 1), (2, 3))"
     * @param entities the columns on the LHS of the relation
     * @param inValues a list of Tuples.Literal instances or a Tuples.Raw markers
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                std::vector<expr::expression> values) {
        return create_multi_column_relation(std::move(entities), expr::oper_t::IN, {}, std::move(values), {});
    }

    /**
     * Creates a multi-column IN relation with a marker for the IN values.
     * For example: "SELECT ... WHERE (a, b) IN ?"
     * @param entities the columns on the LHS of the relation
     * @param inMarker a single IN marker
     * @return a new <code>MultiColumnRelation</code> instance
     */
    static shared_ptr<multi_column_relation> create_single_marker_in_relation(std::vector<shared_ptr<column_identifier::raw>> entities,
                                                                              expr::expression in_marker) {
        return create_multi_column_relation(std::move(entities), expr::oper_t::IN, {}, {}, std::move(in_marker));
    }

    const std::vector<shared_ptr<column_identifier::raw>>& get_entities() const {
        return _entities;
    }

private:
    /**
     * For non-IN relations, returns the Tuples.Literal or Tuples.Raw marker for a single tuple.
     * @return a Tuples.Literal for non-IN relations or Tuples.Raw marker for a single tuple.
     */
    const expr::expression& get_value() {
        return _relation_type == expr::oper_t::IN ? *_in_marker : *_values_or_marker;
    }
public:
    virtual bool is_multi_column() const override { return true; }

protected:
    virtual shared_ptr<restrictions::restriction> new_EQ_restriction(data_dictionary::database db, schema_ptr schema,
                                                                     prepare_context& ctx) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        auto e = to_expression(col_specs, get_value(), db, schema->ks_name(), ctx);
        return ::make_shared<restrictions::multi_column_restriction::EQ>(schema, rs, std::move(e));
    }

    virtual shared_ptr<restrictions::restriction> new_IN_restriction(data_dictionary::database db, schema_ptr schema,
                                                                     prepare_context& ctx) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        if (_in_marker) {
            auto e = to_expression(col_specs, get_value(), db, schema->ks_name(), ctx);
            auto bound_value_marker = expr::as<expr::bind_variable>(e);
            return ::make_shared<restrictions::multi_column_restriction::IN_with_marker>(schema, rs, std::move(bound_value_marker));
        } else {
            std::vector<expr::expression> raws(_in_values.size());
            std::copy(_in_values.begin(), _in_values.end(), raws.begin());
            auto es = to_expressions(col_specs, raws, db, schema->ks_name(), ctx);
            // Convert a single-item IN restriction to an EQ restriction
            if (es.size() == 1) {
                return ::make_shared<restrictions::multi_column_restriction::EQ>(schema, rs, std::move(es[0]));
            }
            return ::make_shared<restrictions::multi_column_restriction::IN_with_values>(schema, rs, std::move(es));
        }
    }

    virtual shared_ptr<restrictions::restriction> new_slice_restriction(data_dictionary::database db, schema_ptr schema,
                                                                        prepare_context& ctx,
                                                                        statements::bound bound, bool inclusive) override {
        auto rs = receivers(db, *schema);
        std::vector<lw_shared_ptr<column_specification>> col_specs(rs.size());
        std::transform(rs.begin(), rs.end(), col_specs.begin(), [] (auto cs) {
            return cs->column_specification;
        });
        auto e = to_expression(col_specs, get_value(), db, schema->ks_name(), ctx);
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema, rs, bound, inclusive, std::move(e), _mode);
    }

    virtual shared_ptr<restrictions::restriction> new_contains_restriction(data_dictionary::database db, schema_ptr schema,
                                                                           prepare_context& ctx, bool is_key) override {
        throw exceptions::invalid_request_exception(format("{} cannot be used for Multi-column relations", get_operator()));
    }

    virtual ::shared_ptr<restrictions::restriction> new_LIKE_restriction(
            data_dictionary::database db, schema_ptr schema, prepare_context& ctx) override {
        throw exceptions::invalid_request_exception("LIKE cannot be used for Multi-column relations");
    }

    virtual ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) override {
        auto new_entities = boost::copy_range<decltype(_entities)>(_entities | boost::adaptors::transformed([&] (auto&& entity) {
            return *entity == from ? ::make_shared<column_identifier::raw>(to) : entity;
        }));
        return create_multi_column_relation(std::move(new_entities), _relation_type, _values_or_marker, _in_values, _in_marker);
    }

    virtual expr::expression to_expression(const std::vector<lw_shared_ptr<column_specification>>& receivers,
                                           const expr::expression& raw, data_dictionary::database db, const sstring& keyspace,
                                           prepare_context& ctx) const override {
        auto e = prepare_expression_multi_column(raw, db, keyspace, receivers);
        expr::fill_prepare_context(e, ctx);
        return e;
    }

    std::vector<const column_definition*> receivers(data_dictionary::database db, const schema& schema) {
        using namespace statements::request_validations;

        int previous_position = -1;
        std::vector<const column_definition*> names;
        for (auto&& raw : get_entities()) {
            const auto& def = to_column_definition(schema, *raw);
            check_true(def.is_clustering_key(), "Multi-column relations can only be applied to clustering columns but was applied to: {}", def.name_as_text());
            check_false(std::count(names.begin(), names.end(), &def), "Column \"{}\" appeared twice in a relation: {}", def.name_as_text(), to_string());

            // FIXME: the following restriction should be removed (CASSANDRA-8613)
            if (def.position() != unsigned(previous_position + 1)) {
                check_false(previous_position == -1, "Clustering columns may not be skipped in multi-column relations. "
                                                     "They should appear in the PRIMARY KEY order. Got {}", to_string());
                throw exceptions::invalid_request_exception(format("Clustering columns must appear in the PRIMARY KEY order in multi-column relations: {}", to_string()));
            }
            names.emplace_back(&def);
            previous_position = def.position();
        }
        return names;
    }

    template <typename T>
    static sstring tuple_to_string(const std::vector<T>& items) {
        return format("({})", join(", ", items));
    }

    virtual sstring to_string() const override {
        sstring str = tuple_to_string(_entities);
        if (is_IN()) {
            str += " IN ";
            str += !_in_marker ? "?" : tuple_to_string(_in_values);
            return str;
        }
        return format("{} {} {}", str, _relation_type, *_values_or_marker);
    }
};

}
