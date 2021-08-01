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

#include "cql3/prepare_context.hh"
#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function_call.hh"

namespace cql3 {

size_t prepare_context::bound_variables_size() const {
    return _variable_names.size();
}

const std::vector<lw_shared_ptr<column_specification>>& prepare_context::get_variable_specifications() const & {
    return _specs;
}

std::vector<lw_shared_ptr<column_specification>> prepare_context::get_variable_specifications() && {
    return std::move(_specs);
}

std::vector<uint16_t> prepare_context::get_partition_key_bind_indexes(const schema& schema) const {
    auto count = schema.partition_key_columns().size();
    std::vector<uint16_t> partition_key_positions(count, uint16_t(0));
    std::vector<bool> set(count, false);
    for (size_t i = 0; i < _target_columns.size(); i++) {
        auto& target_column = _target_columns[i];
        const auto* cdef = target_column ? schema.get_column_definition(target_column->name->name()) : nullptr;
        if (cdef && cdef->is_partition_key()) {
            partition_key_positions[cdef->position()] = i;
            set[cdef->position()] = true;
        }
    }
    for (bool b : set) {
        if (!b) {
            return {};
        }
    }
    return partition_key_positions;
}

void prepare_context::add_variable_specification(int32_t bind_index, lw_shared_ptr<column_specification> spec) {
    _target_columns[bind_index] = spec;
    auto name = _variable_names[bind_index];
    // Use the user name, if there is one
    if (name) {
        spec = make_lw_shared<column_specification>(spec->ks_name, spec->cf_name, name, spec->type);
    }
    _specs[bind_index] = spec;
}

void prepare_context::set_bound_variables(const std::vector<shared_ptr<column_identifier>>& prepare_meta) {
    _variable_names = prepare_meta;
    _specs.clear();
    _target_columns.clear();

    const size_t bn_size = prepare_meta.size();
    _specs.resize(bn_size);
    _target_columns.resize(bn_size);
}

prepare_context::function_calls_t& prepare_context::pk_function_calls() {
    return _pk_fn_calls;
}

void prepare_context::add_pk_function_call(::shared_ptr<cql3::functions::function_call> fn) {
    constexpr auto fn_limit = std::numeric_limits<uint8_t>::max();
    if (_pk_fn_calls.size() == fn_limit) {
        throw exceptions::invalid_request_exception(
            format("Too many function calls within one statement. Max supported number is {}", fn_limit));
    }
    fn->set_id(_pk_fn_calls.size());
    _pk_fn_calls.emplace_back(std::move(fn));
}


}
