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

#include <vector>

#include "cql3/query_options.hh"
#include "cql3/statements/bound.hh"
#include "cql3/restrictions/restrictions.hh"
#include "cql3/restrictions/restriction.hh"
#include "cql3/restrictions/restriction.hh"
#include "types.hh"
#include "query-request.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {
namespace restrictions {

/**
 * A set of restrictions on a primary key part (partition key or clustering key).
 *
 * What was in AbstractPrimaryKeyRestrictions was moved here (In pre 1.8 Java interfaces could not have default
 * implementations of methods).
 */

class partition_key_restrictions: public restriction, public restrictions, public enable_shared_from_this<partition_key_restrictions> {
public:
    partition_key_restrictions() = default;

    virtual void merge_with(::shared_ptr<restriction> other) = 0;

    virtual ::shared_ptr<partition_key_restrictions> merge_to(schema_ptr, ::shared_ptr<restriction> restriction) {
        merge_with(restriction);
        return this->shared_from_this();
    }

    using restrictions::has_supporting_index;

    bool empty() const override {
        return get_column_defs().empty();
    }
    uint32_t size() const override {
        return uint32_t(get_column_defs().size());
    }

    bool has_unrestricted_components(const schema& schema) const {
        return size() < schema.partition_key_size();
    }

    virtual bool needs_filtering(const schema& schema) const {
        return !empty() && !has_token(expression) &&
                (has_unrestricted_components(schema) || has_slice_or_needs_filtering(expression));
    }

    // NOTICE(sarna): This function is useless for partition key restrictions,
    // but it should remain here until single_column_primary_key_restrictions class is detemplatized.
    virtual unsigned int num_prefix_columns_that_need_not_be_filtered() const {
        return 0;
    }

    virtual bool is_all_eq() const {
        return false;
    }

    size_t prefix_size(const schema&) const {
        return 0;
    }
};

class clustering_key_restrictions : public restriction, public restrictions, public enable_shared_from_this<clustering_key_restrictions> {
public:
    clustering_key_restrictions() = default;

    virtual void merge_with(::shared_ptr<restriction> other) = 0;

    virtual ::shared_ptr<clustering_key_restrictions> merge_to(schema_ptr, ::shared_ptr<restriction> restriction) {
        merge_with(restriction);
        return this->shared_from_this();
    }

    using restrictions::has_supporting_index;

    bool empty() const override {
        return get_column_defs().empty();
    }
    uint32_t size() const override {
        return uint32_t(get_column_defs().size());
    }

    bool has_unrestricted_components(const schema& schema) const {
        return size() < schema.clustering_key_size();
    }

    virtual bool needs_filtering(const schema& schema) const {
        return false;
    }

    // How long a prefix of the restrictions could have resulted in
    // need_filtering() == false. These restrictions do not need to be
    // applied during filtering.
    // For example, if we have the filter "c1 < 3 and c2 > 3", c1 does
    // not need filtering (just a read stopping at c1=3) but c2 does,
    // so num_prefix_columns_that_need_not_be_filtered() will be 1.
    virtual unsigned int num_prefix_columns_that_need_not_be_filtered() const {
        return 0;
    }

    virtual bool is_all_eq() const {
        return false;
    }

    size_t prefix_size(const schema& schema) const {
        size_t count = 0;
        if (schema.clustering_key_columns().empty()) {
            return count;
        }
        auto column_defs = get_column_defs();
        column_id expected_column_id = schema.clustering_key_columns().begin()->id;
        for (auto&& cdef : column_defs) {
            if (schema.position(*cdef) != expected_column_id) {
                return count;
            }
            expected_column_id++;
            count++;
        }
        return count;
    }
};

// FIXME(sarna): transitive hack only, do not judge. Should be dropped after all primary_key_restrictions<T> uses are removed from code.
template<typename ValueType>
using primary_key_restrictions = std::conditional_t<std::is_same_v<ValueType, partition_key>, partition_key_restrictions, clustering_key_restrictions>;


}
}
