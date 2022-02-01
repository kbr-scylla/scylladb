/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
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
        snitch_ptr& snitch,
        const replication_strategy_config_options& config_options);

    virtual size_t get_replication_factor(const token_metadata&) const override {
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
    virtual future<inet_address_vector_replica_set> calculate_natural_endpoints(
        const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options() const override;

    virtual std::optional<std::set<sstring>> recognized_options(const topology&) const override;

private:
    // map: data centers -> replication factor
    std::unordered_map<sstring, size_t> _dc_rep_factor;

    std::vector<sstring> _datacenteres;
    size_t _rep_factor;
};
} // namespace locator
