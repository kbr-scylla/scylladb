/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "snitch_base.hh"
#include "utils/fb_utilities.hh"

namespace locator {

using inet_address = gms::inet_address;

/**
 * A simple endpoint snitch implementation that assumes datacenter and rack information is encoded
 * in the 2nd and 3rd octets of the ip address, respectively.
 */
struct rack_inferring_snitch : public snitch_base {
    rack_inferring_snitch(const snitch_config& cfg) {
        _my_dc = get_datacenter(utils::fb_utilities::get_broadcast_address());
        _my_rack = get_rack(utils::fb_utilities::get_broadcast_address());

        // This snitch is ready on creation
        set_snitch_ready();
    }

    virtual sstring get_rack(inet_address endpoint) override {
        return std::to_string(uint8_t(endpoint.bytes()[2]));
    }

    virtual sstring get_datacenter(inet_address endpoint) override {
        return std::to_string(uint8_t(endpoint.bytes()[1]));
    }

    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.RackInferringSnitch";
    }
};

} // namespace locator
