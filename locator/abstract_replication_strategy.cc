/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "locator/abstract_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/stall_free.hh"

namespace locator {

logging::logger rslogger("replication_strategy");

abstract_replication_strategy::abstract_replication_strategy(
    snitch_ptr& snitch,
    const replication_strategy_config_options& config_options,
    replication_strategy_type my_type)
        : _config_options(config_options)
        , _snitch(snitch)
        , _my_type(my_type) {}

abstract_replication_strategy::ptr_type abstract_replication_strategy::create_replication_strategy(const sstring& strategy_name, const replication_strategy_config_options& config_options) {
    assert(locator::i_endpoint_snitch::get_local_snitch_ptr());
    try {
        return create_object<abstract_replication_strategy,
                             snitch_ptr&,
                             const replication_strategy_config_options&>
            (strategy_name,
             locator::i_endpoint_snitch::get_local_snitch_ptr(), config_options);
    } catch (const no_such_class& e) {
        throw exceptions::configuration_exception(e.what());
    }
}

void abstract_replication_strategy::validate_replication_strategy(const sstring& ks_name,
                                                                  const sstring& strategy_name,
                                                                  const replication_strategy_config_options& config_options,
                                                                  const topology& topology)
{
    auto strategy = create_replication_strategy(strategy_name, config_options);
    strategy->validate_options();
    auto expected = strategy->recognized_options(topology);
    if (expected) {
        for (auto&& item : config_options) {
            sstring key = item.first;
            if (!expected->contains(key)) {
                 throw exceptions::configuration_exception(format("Unrecognized strategy option {{{}}} passed to {} for keyspace {}", key, strategy_name, ks_name));
            }
        }
    }
}

inet_address_vector_replica_set abstract_replication_strategy::get_natural_endpoints(const token& search_token, const effective_replication_map& erm) const {
    const token& key_token = erm.get_token_metadata_ptr()->first_token(search_token);
    auto res = erm.get_replication_map().find(key_token);
    return res->second;
}

inet_address_vector_replica_set effective_replication_map::get_natural_endpoints_without_node_being_replaced(const token& search_token) const {
    inet_address_vector_replica_set natural_endpoints = get_natural_endpoints(search_token);
    if (_tmptr->is_any_node_being_replaced() &&
        _rs->allow_remove_node_being_replaced_from_natural_endpoints()) {
        // When a new node is started to replace an existing dead node, we want
        // to make the replacing node take writes but do not count it for
        // consistency level, because the replacing node can die and go away.
        // To do this, we filter out the existing node being replaced from
        // natural_endpoints and make the replacing node in the pending_endpoints.
        //
        // However, we can only apply the filter for the replication strategy
        // that allows it. For example, we can not apply the filter for
        // LocalStrategy because LocalStrategy always returns the node itself
        // as the natural_endpoints and the node will not appear in the
        // pending_endpoints.
        auto it = boost::range::remove_if(natural_endpoints, [this] (gms::inet_address& p) {
            return _tmptr->is_being_replaced(p);
        });
        natural_endpoints.erase(it, natural_endpoints.end());
    }
    return natural_endpoints;
}

void abstract_replication_strategy::validate_replication_factor(sstring rf)
{
    if (rf.empty() || std::any_of(rf.begin(), rf.end(), [] (char c) {return !isdigit(c);})) {
        throw exceptions::configuration_exception(
                format("Replication factor must be numeric and non-negative, found '{}'", rf));
    }
    try {
        std::stol(rf);
    } catch (...) {
        throw exceptions::configuration_exception(
            sstring("Replication factor must be numeric; found ") + rf);
    }
}

static
void
insert_token_range_to_sorted_container_while_unwrapping(
        const dht::token& prev_tok,
        const dht::token& tok,
        dht::token_range_vector& ret) {
    if (prev_tok < tok) {
        auto pos = ret.end();
        if (!ret.empty() && !std::prev(pos)->end()) {
            // We inserted a wrapped range (a, b] previously as
            // (-inf, b], (a, +inf). So now we insert in the next-to-last
            // position to keep the last range (a, +inf) at the end.
            pos = std::prev(pos);
        }
        ret.insert(pos,
                dht::token_range{
                        dht::token_range::bound(prev_tok, false),
                        dht::token_range::bound(tok, true)});
    } else {
        ret.emplace_back(
                dht::token_range::bound(prev_tok, false),
                std::nullopt);
        // Insert in front to maintain sorded order
        ret.emplace(
                ret.begin(),
                std::nullopt,
                dht::token_range::bound(tok, true));
    }
}

dht::token_range_vector
effective_replication_map::do_get_ranges(noncopyable_function<bool(inet_address_vector_replica_set)> should_add_range) const {
    dht::token_range_vector ret;
    const auto& tm = *_tmptr;
    auto prev_tok = tm.sorted_tokens().back();
    for (const auto& tok : tm.sorted_tokens()) {
        if (should_add_range(get_natural_endpoints(tok))) {
            insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
        }
        prev_tok = tok;
    }
    return ret;
}

dht::token_range_vector
effective_replication_map::get_ranges(inet_address ep) const {
    return do_get_ranges([ep] (inet_address_vector_replica_set eps) {
        for (auto a : eps) {
            if (a == ep) {
                return true;
            }
        }
        return false;
    });
}

// Caller must ensure that token_metadata will not change throughout the call if can_yield::yes.
future<dht::token_range_vector>
abstract_replication_strategy::get_ranges(inet_address ep, token_metadata_ptr tmptr) const {
    dht::token_range_vector ret;
    const auto& tm = *tmptr;
    auto prev_tok = tm.sorted_tokens().back();
    for (auto tok : tm.sorted_tokens()) {
        for (inet_address a : co_await calculate_natural_endpoints(tok, tm)) {
            if (a == ep) {
                insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
                break;
            }
        }
        prev_tok = tok;
    }
    co_return ret;
}

dht::token_range_vector
effective_replication_map::get_primary_ranges(inet_address ep) const {
    return do_get_ranges([ep] (inet_address_vector_replica_set eps) {
        return eps.size() > 0 && eps[0] == ep;
    });
}

dht::token_range_vector
effective_replication_map::get_primary_ranges_within_dc(inet_address ep) const {
    sstring local_dc = _rs->_snitch->get_datacenter(ep);
    std::unordered_set<inet_address> local_dc_nodes = _tmptr->get_topology().get_datacenter_endpoints().at(local_dc);
    return do_get_ranges([ep, local_dc_nodes = std::move(local_dc_nodes)] (inet_address_vector_replica_set eps) {
        // Unlike get_primary_ranges() which checks if ep is the first
        // owner of this range, here we check if ep is the first just
        // among nodes which belong to the local dc of ep.
        for (auto& e : eps) {
            if (local_dc_nodes.contains(e)) {
                return e == ep;
            }
        }
        return false;
    });
}

future<std::unordered_multimap<inet_address, dht::token_range>>
abstract_replication_strategy::get_address_ranges(const token_metadata& tm) const {
    std::unordered_multimap<inet_address, dht::token_range> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector r = tm.get_primary_ranges_for(t);
        auto eps = co_await calculate_natural_endpoints(t, tm);
        rslogger.debug("token={}, primary_range={}, address={}", t, r, eps);
        for (auto ep : eps) {
            for (auto&& rng : r) {
                ret.emplace(ep, rng);
            }
        }
    }
    co_return ret;
}

future<std::unordered_multimap<inet_address, dht::token_range>>
abstract_replication_strategy::get_address_ranges(const token_metadata& tm, inet_address endpoint) const {
    std::unordered_multimap<inet_address, dht::token_range> ret;
    for (auto& t : tm.sorted_tokens()) {
        auto eps = co_await calculate_natural_endpoints(t, tm);
        bool found = false;
        for (auto ep : eps) {
            if (ep != endpoint) {
                continue;
            }
            dht::token_range_vector r = tm.get_primary_ranges_for(t);
            rslogger.debug("token={} primary_range={} endpoint={}", t, r, endpoint);
            for (auto&& rng : r) {
                ret.emplace(ep, rng);
            }
            found = true;
            break;
        }
        if (!found) {
            rslogger.debug("token={} natural_endpoints={}: endpoint={} not found", t, eps, endpoint);
        }
    }
    co_return ret;
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
effective_replication_map::get_range_addresses() const {
    const token_metadata& tm = *_tmptr;
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        auto eps = get_natural_endpoints(t);
        for (auto& r : ranges) {
            ret.emplace(r, eps);
        }
    }
    return ret;
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
abstract_replication_strategy::get_range_addresses(const token_metadata& tm) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        auto eps = co_await calculate_natural_endpoints(t, tm);
        for (auto& r : ranges) {
            ret.emplace(r, eps);
        }
    }
    co_return ret;
}

future<dht::token_range_vector>
abstract_replication_strategy::get_pending_address_ranges(const token_metadata_ptr tmptr, token pending_token, inet_address pending_address) const {
    return get_pending_address_ranges(std::move(tmptr), std::unordered_set<token>{pending_token}, pending_address);
}

future<dht::token_range_vector>
abstract_replication_strategy::get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address) const {
    dht::token_range_vector ret;
    token_metadata temp;
    temp = co_await tmptr->clone_only_token_map();
    co_await temp.update_normal_tokens(pending_tokens, pending_address);
    for (auto& x : co_await get_address_ranges(temp, pending_address)) {
            ret.push_back(x.second);
    }
    co_await temp.clear_gently();
    co_return ret;
}

future<mutable_effective_replication_map_ptr> calculate_effective_replication_map(abstract_replication_strategy::ptr_type rs, token_metadata_ptr tmptr) {
    replication_map replication_map;

    for (const auto &t : tmptr->sorted_tokens()) {
        replication_map.emplace(t, co_await rs->calculate_natural_endpoints(t, *tmptr));
    }

    auto rf = rs->get_replication_factor(*tmptr);
    co_return make_effective_replication_map(std::move(rs), std::move(tmptr), std::move(replication_map), rf);
}

future<replication_map> effective_replication_map::clone_endpoints_gently() const {
    replication_map cloned_endpoints;

    for (auto& i : _replication_map) {
        cloned_endpoints.emplace(i.first, i.second);
        co_await coroutine::maybe_yield();
    }

    co_return cloned_endpoints;
}

inet_address_vector_replica_set effective_replication_map::get_natural_endpoints(const token& search_token) const {
    return _rs->get_natural_endpoints(search_token, *this);
}

future<> effective_replication_map::clear_gently() noexcept {
    co_await utils::clear_gently(_replication_map);
    co_await utils::clear_gently(_tmptr);
}

} // namespace locator
