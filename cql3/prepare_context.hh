/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

#include <optional>
#include <vector>
#include <stddef.h>
#include "cql3/expr/expression.hh"

class schema;

namespace cql3 {

class column_identifier;
class column_specification;
namespace functions { class function_call; }

/**
 * Metadata class currently holding bind variables specifications and
 * `function_call` AST nodes inside a query partition key restrictions.
 * Populated and maintained at "prepare" step of query execution.
 */
class prepare_context final {
private:
    std::vector<shared_ptr<column_identifier>> _variable_names;
    std::vector<lw_shared_ptr<column_specification>> _specs;
    std::vector<lw_shared_ptr<column_specification>> _target_columns;
    // A list of pointers to prepared `function_call` cache ids, that
    // participate in partition key ranges computation within an LWT statement.
    std::vector<::shared_ptr<std::optional<uint8_t>>> _pk_function_calls_cache_ids;

    // The flag denoting whether the context is currently in partition key
    // processing mode (inside query restrictions AST nodes). If set to true,
    // then every `function_call` instance will be recorded in the context and
    // will be assigned an identifier, which will then be used for caching
    // the function call results.
    bool _processing_pk_restrictions = false;

public:

    prepare_context() = default;

    size_t bound_variables_size() const;

    const std::vector<lw_shared_ptr<column_specification>>& get_variable_specifications() const &;

    std::vector<lw_shared_ptr<column_specification>> get_variable_specifications() &&;

    std::vector<uint16_t> get_partition_key_bind_indexes(const schema& schema) const;

    void add_variable_specification(int32_t bind_index, lw_shared_ptr<column_specification> spec);

    void set_bound_variables(const std::vector<shared_ptr<column_identifier>>& prepare_meta);

    void clear_pk_function_calls_cache();

    // Record a new function call, which evaluates a partition key constraint.
    // Also automatically assigns an id to the AST node for caching purposes.
    void add_pk_function_call(cql3::expr::function_call& fn);

    // Inform the context object that it has started or ended processing the
    // partition key part of statement restrictions.
    void set_processing_pk_restrictions(bool flag) noexcept {
        _processing_pk_restrictions = flag;
    }

    bool is_processing_pk_restrictions() const noexcept {
        return _processing_pk_restrictions;
    }
};

}
