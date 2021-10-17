/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <algorithm>
#include "local_strategy.hh"
#include "utils/class_registrator.hh"
#include "utils/fb_utilities.hh"


namespace locator {

local_strategy::local_strategy(snitch_ptr& snitch, const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(snitch, config_options, replication_strategy_type::local) {}

future<inet_address_vector_replica_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<inet_address_vector_replica_set>(inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()}));
}

void local_strategy::validate_options() const {
}

std::optional<std::set<sstring>> local_strategy::recognized_options(const topology&) const {
    // LocalStrategy doesn't expect any options.
    return {};
}

size_t local_strategy::get_replication_factor(const token_metadata&) const {
    return 1;
}

inet_address_vector_replica_set local_strategy::get_natural_endpoints(const token&, const effective_replication_map&) const {
    return inet_address_vector_replica_set({utils::fb_utilities::get_broadcast_address()});
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, snitch_ptr&, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
