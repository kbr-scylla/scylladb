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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "locator/abstract_replication_strategy.hh"
#include "exceptions/exceptions.hh"

#include <optional>
#include <set>

namespace locator {
class network_topology_strategy : public abstract_replication_strategy {
public:
    network_topology_strategy(
        const shared_token_metadata& token_metadata,
        snitch_ptr& snitch,
        const std::map<sstring,sstring>& config_options);

    virtual size_t get_replication_factor() const override {
        return _rep_factor;
    }

    size_t get_replication_factor(const sstring& dc) const {
        auto dc_factor = _dc_rep_factor.find(dc);
        return (dc_factor == _dc_rep_factor.end()) ? 0 : dc_factor->second;
    }

    const std::vector<sstring>& get_datacenters() const {
        return _datacenteres;
    }

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

protected:
    /**
     * calculate endpoints in one pass through the tokens by tracking our
     * progress in each DC, rack etc.
     */
    virtual inet_address_vector_replica_set calculate_natural_endpoints(
        const token& search_token, const token_metadata& tm, can_yield) const override;

    virtual void validate_options() const override;

    virtual std::optional<std::set<sstring>> recognized_options() const override;

private:
    // map: data centers -> replication factor
    std::unordered_map<sstring, size_t> _dc_rep_factor;

    std::vector<sstring> _datacenteres;
    size_t _rep_factor;
};
} // namespace locator
