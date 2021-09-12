/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <algorithm>
#include "simple_strategy.hh"
#include "utils/class_registrator.hh"
#include <boost/algorithm/string.hpp>
#include "utils/sequenced_set.hh"

namespace locator {

simple_strategy::simple_strategy(const shared_token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
        abstract_replication_strategy(token_metadata, snitch, config_options, replication_strategy_type::simple) {
    for (auto& config_pair : config_options) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        if (boost::iequals(key, "replication_factor")) {
            validate_replication_factor(val);
            _replication_factor = std::stol(val);

            break;
        }
    }
}

inet_address_vector_replica_set simple_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm, can_yield can_yield) const {
    const std::vector<token>& tokens = tm.sorted_tokens();

    if (tokens.empty()) {
        return inet_address_vector_replica_set();
    }

    size_t replicas = get_replication_factor();
    utils::sequenced_set<inet_address> endpoints;
    endpoints.reserve(replicas);

    for (auto& token : tm.ring_range(t)) {
        if (endpoints.size() == replicas) {
           break;
        }
        if (can_yield) {
            seastar::thread::maybe_yield();
        }
        auto ep = tm.get_endpoint(token);
        assert(ep);

        endpoints.push_back(*ep);
    }

    return boost::copy_range<inet_address_vector_replica_set>(endpoints.get_vector());
}

size_t simple_strategy::get_replication_factor() const {
    return _replication_factor;
}

void simple_strategy::validate_options() const {
    auto it = _config_options.find("replication_factor");
    if (it == _config_options.end()) {
        throw exceptions::configuration_exception("SimpleStrategy requires a replication_factor strategy option.");
    }
    validate_replication_factor(it->second);
}

std::optional<std::set<sstring>>simple_strategy::recognized_options() const {
    return {{ "replication_factor" }};
}

using registry = class_registrator<abstract_replication_strategy, simple_strategy, const shared_token_metadata&, snitch_ptr&, const std::map<sstring, sstring>&>;
static registry registrator("org.apache.cassandra.locator.SimpleStrategy");
static registry registrator_short_name("SimpleStrategy");

}
