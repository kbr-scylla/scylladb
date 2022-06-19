/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "cql3/prepare_context.hh"
#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"

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
    auto name = _variable_names[bind_index];
    if (_specs[bind_index]) {
        // If the same variable is used in multiple places, check that the types are compatible
        if (&spec->type->without_reversed() != &_specs[bind_index]->type->without_reversed()) {
            throw exceptions::invalid_request_exception(
                    fmt::format("variable :{} has type {} which doesn't match {}",
                            *name, _specs[bind_index]->type->as_cql3_type(), spec->name));
        }
    }
    _target_columns[bind_index] = spec;
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

void prepare_context::clear_pk_function_calls_cache() {
    for (::shared_ptr<std::optional<uint8_t>>& cache_id : _pk_function_calls_cache_ids) {
        if (cache_id.get() != nullptr) {
            *cache_id = std::nullopt;
        }
    }
}

void prepare_context::add_pk_function_call(expr::function_call& fn) {
    constexpr auto fn_limit = std::numeric_limits<uint8_t>::max();
    if (_pk_function_calls_cache_ids.size() == fn_limit) {
        throw exceptions::invalid_request_exception(
            format("Too many function calls within one statement. Max supported number is {}", fn_limit));
    }

    fn.lwt_cache_id = ::make_shared<std::optional<uint8_t>>(_pk_function_calls_cache_ids.size());
    _pk_function_calls_cache_ids.emplace_back(fn.lwt_cache_id);
}


}
