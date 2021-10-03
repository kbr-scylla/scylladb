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

#include "cql3/abstract_marker.hh"
#include "maps.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "to_string.hh"
#include <unordered_set>

namespace cql3 {

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification& column);

    class value : public terminal, collection_terminal {
    public:
        std::set<managed_bytes, serialized_compare> _elements;
    public:
        value(std::set<managed_bytes, serialized_compare> elements, data_type my_type)
                : terminal(std::move(my_type)), _elements(std::move(elements)) {
        }
        static value from_serialized(const raw_value_view& v, const set_type_impl& type, cql_serialization_format sf);
        virtual cql3::raw_value get(const query_options& options) override;
        virtual managed_bytes get_with_protocol_version(cql_serialization_format sf) override;
        bool equals(const set_type_impl& st, const value& v);
        virtual sstring to_string() const override;
    };

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        std::vector<shared_ptr<term>> _elements;
        data_type _my_type;
    public:
        delayed_value(std::vector<shared_ptr<term>> elements, data_type my_type)
            : _elements(std::move(elements)), _my_type(std::move(my_type)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void fill_prepare_context(prepare_context& ctx) const override;
        virtual shared_ptr<terminal> bind(const query_options& options) override;

        virtual expr::expression to_expression() override;
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver);
        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
        virtual expr::expression to_expression() override;
    };

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const expr::constant& value);
    };

    class adder : public operation {
    public:
        adder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
                const expr::constant& value, const column_definition& column);
    };

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    class discarder : public operation {
    public:
        discarder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class element_discarder : public operation {
    public:
        element_discarder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) { }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
