/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/on_internal_error.hh>

#include "log.hh"
#include "locator/topology.hh"
#include "utils/stall_free.hh"
#include "utils/fb_utilities.hh"

namespace locator {

static logging::logger tlogger("topology");

future<> topology::clear_gently() noexcept {
    co_await utils::clear_gently(_dc_endpoints);
    co_await utils::clear_gently(_dc_racks);
    co_await utils::clear_gently(_current_locations);
    co_await utils::clear_gently(_pending_locations);
    _datacenters.clear();
    co_return;
}

topology::topology(config cfg)
        : _sort_by_proximity(!cfg.disable_proximity_sorting)
{
    _pending_locations[utils::fb_utilities::get_broadcast_address()] = std::move(cfg.local_dc_rack);
}

future<topology> topology::clone_gently() const {
    topology ret;
    ret._dc_endpoints.reserve(_dc_endpoints.size());
    for (const auto& p : _dc_endpoints) {
        ret._dc_endpoints.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._dc_racks.reserve(_dc_racks.size());
    for (const auto& [dc, rack_endpoints] : _dc_racks) {
        ret._dc_racks[dc].reserve(rack_endpoints.size());
        for (const auto& p : rack_endpoints) {
            ret._dc_racks[dc].emplace(p);
        }
    }
    co_await coroutine::maybe_yield();
    ret._current_locations.reserve(_current_locations.size());
    for (const auto& p : _current_locations) {
        ret._current_locations.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._pending_locations.reserve(_pending_locations.size());
    for (const auto& p : _pending_locations) {
        ret._pending_locations.emplace(p);
    }
    co_await coroutine::maybe_yield();
    ret._datacenters = _datacenters;
    ret._sort_by_proximity = _sort_by_proximity;
    co_return ret;
}

void topology::remove_pending_location(const inet_address& ep) {
    if (ep != utils::fb_utilities::get_broadcast_address()) {
        _pending_locations.erase(ep);
    }
}

void topology::update_endpoint(const inet_address& ep, endpoint_dc_rack dr, pending pend)
{
    if (pend) {
        _pending_locations[ep] = std::move(dr);
        return;
    }

    auto current = _current_locations.find(ep);

    if (current != _current_locations.end()) {
        if (current->second.dc == dr.dc && current->second.rack == dr.rack) {
            return;
        }
        remove_endpoint(ep);
    }

    _dc_endpoints[dr.dc].insert(ep);
    _dc_racks[dr.dc][dr.rack].insert(ep);
    _datacenters.insert(dr.dc);
    _current_locations[ep] = std::move(dr);
    remove_pending_location(ep);
}

void topology::remove_endpoint(inet_address ep)
{
    auto cur_dc_rack = _current_locations.find(ep);

    if (cur_dc_rack == _current_locations.end()) {
        remove_pending_location(ep);
        return;
    }

    const auto& dc = cur_dc_rack->second.dc;
    const auto& rack = cur_dc_rack->second.rack;
    if (auto dit = _dc_endpoints.find(dc); dit != _dc_endpoints.end()) {
        auto& eps = dit->second;
        eps.erase(ep);
        if (eps.empty()) {
            _dc_endpoints.erase(dit);
            _datacenters.erase(dc);
            _dc_racks.erase(dc);
        } else {
            auto& racks = _dc_racks[dc];
            if (auto rit = racks.find(rack); rit != racks.end()) {
                eps = rit->second;
                eps.erase(ep);
                if (eps.empty()) {
                    racks.erase(rit);
                }
            }
        }
    }

    _current_locations.erase(cur_dc_rack);
}

bool topology::has_endpoint(inet_address ep, pending with_pending) const
{
    return _current_locations.contains(ep) || (with_pending && _pending_locations.contains(ep));
}

const endpoint_dc_rack& topology::get_location(const inet_address& ep) const {
    if (_current_locations.contains(ep)) {
        return _current_locations.at(ep);
    }

    if (_pending_locations.contains(ep)) {
        return _pending_locations.at(ep);
    }

    on_internal_error(tlogger, format("Node {} is not in topology", ep));
}

// FIXME -- both methods below should rather return data from the
// get_location() result, but to make it work two things are to be fixed:
// - topology should be aware of internal-ip conversions
// - topology should be pre-populated with data loaded from system ks

sstring topology::get_rack() const {
    return get_rack(utils::fb_utilities::get_broadcast_address());
}

sstring topology::get_rack(inet_address ep) const {
    return get_location(ep).rack;
}

sstring topology::get_datacenter() const {
    return get_datacenter(utils::fb_utilities::get_broadcast_address());
}

sstring topology::get_datacenter(inet_address ep) const {
    return get_location(ep).dc;
}

void topology::sort_by_proximity(inet_address address, inet_address_vector_replica_set& addresses) const {
    if (_sort_by_proximity) {
        std::sort(addresses.begin(), addresses.end(), [this, &address](inet_address& a1, inet_address& a2) {
            return compare_endpoints(address, a1, a2) < 0;
        });
    }
}

int topology::compare_endpoints(const inet_address& address, const inet_address& a1, const inet_address& a2) const {
    //
    // if one of the Nodes IS the Node we are comparing to and the other one
    // IS NOT - then return the appropriate result.
    //
    if (address == a1 && address != a2) {
        return -1;
    }

    if (address == a2 && address != a1) {
        return 1;
    }

    // ...otherwise perform the similar check in regard to Data Center
    sstring address_datacenter = get_datacenter(address);
    sstring a1_datacenter = get_datacenter(a1);
    sstring a2_datacenter = get_datacenter(a2);

    if (address_datacenter == a1_datacenter &&
        address_datacenter != a2_datacenter) {
        return -1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter != a1_datacenter) {
        return 1;
    } else if (address_datacenter == a2_datacenter &&
               address_datacenter == a1_datacenter) {
        //
        // ...otherwise (in case Nodes belong to the same Data Center) check
        // the racks they belong to.
        //
        sstring address_rack = get_rack(address);
        sstring a1_rack = get_rack(a1);
        sstring a2_rack = get_rack(a2);

        if (address_rack == a1_rack && address_rack != a2_rack) {
            return -1;
        }

        if (address_rack == a2_rack && address_rack != a1_rack) {
            return 1;
        }
    }
    //
    // We don't differentiate between Nodes if all Nodes belong to different
    // Data Centers, thus make them equal.
    //
    return 0;
}

} // namespace locator
