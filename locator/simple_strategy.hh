/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "abstract_replication_strategy.hh"

#include <optional>
#include <set>

namespace locator {

class simple_strategy : public abstract_replication_strategy {
public:
    simple_strategy(snitch_ptr& snitch, const replication_strategy_config_options& config_options);
    virtual ~simple_strategy() {};
    virtual size_t get_replication_factor(const token_metadata& tm) const override;
    virtual void validate_options() const override;
    virtual std::optional<std::set<sstring>> recognized_options(const topology&) const override;
    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const override {
        return true;
    }

    virtual future<inet_address_vector_replica_set> calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const override;
private:
    size_t _replication_factor = 1;
};

}
