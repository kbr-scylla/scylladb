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
#include <optional>

namespace locator {
class everywhere_replication_strategy : public abstract_replication_strategy {
public:
    everywhere_replication_strategy(snitch_ptr& snitch, const replication_strategy_config_options& config_options);

    virtual future<inet_address_vector_replica_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;

    virtual void validate_options() const override { /* noop */ }

    std::optional<std::set<sstring>> recognized_options(const topology&) const override {
        // We explicitely allow all options
        return std::nullopt;
    }

    virtual size_t get_replication_factor(const token_metadata& tm) const override;

    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

    /**
     * We need to override this because the default implementation depends
     * on token calculations but everywhere_replication_strategy may be used before tokens are set up.
     */
    virtual inet_address_vector_replica_set get_natural_endpoints(const token&, const effective_replication_map&) const override;
};
}
