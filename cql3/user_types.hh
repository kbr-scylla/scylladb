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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/abstract_marker.hh"
#include "column_specification.hh"
#include "term.hh"
#include "column_identifier.hh"
#include "operation.hh"
#include "to_string.hh"

namespace cql3 {

/**
 * Static helper methods and classes for user types.
 */
class user_types {
    user_types() = delete;
public:
    static lw_shared_ptr<column_specification> field_spec_of(const column_specification& column, size_t field);

    class value : public terminal {
        std::vector<managed_bytes_opt> _elements;
    public:
        explicit value(std::vector<managed_bytes_opt>, data_type my_type);

        static value from_serialized(const raw_value_view&, const user_type_impl&);

        virtual cql3::raw_value get(const query_options&) override;
        virtual sstring to_string() const override;
    };

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    class delayed_value : public non_terminal {
        user_type _type;
        std::vector<shared_ptr<term>> _values;
    public:
        delayed_value(user_type type, std::vector<shared_ptr<term>> values);
        virtual bool contains_bind_marker() const override;
        virtual void fill_prepare_context(prepare_context& ctx) const override;
    private:
        std::vector<managed_bytes_opt> bind_internal(const query_options& options);
    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override;

        virtual expr::expression to_expression() override;
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(_receiver->type->is_user_type());
        }

        virtual shared_ptr<terminal> bind(const query_options& options) override;
        virtual expr::expression to_expression() override;
    };

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const expr::constant& value);
    };

    class setter_by_field : public operation {
        size_t _field_idx;
    public:
        setter_by_field(const column_definition& column, size_t field_idx, shared_ptr<term> t)
            : operation(column, std::move(t)), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class deleter_by_field : public operation {
        size_t _field_idx;
    public:
        deleter_by_field(const column_definition& column, size_t field_idx)
            : operation(column, nullptr), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
