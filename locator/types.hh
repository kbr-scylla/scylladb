/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "gms/inet_address.hh"
#include "locator/host_id.hh"

using namespace seastar;

namespace locator {

using inet_address = gms::inet_address;

// Endpoint Data Center and Rack names
struct endpoint_dc_rack {
    sstring dc;
    sstring rack;
};

using dc_rack_fn = seastar::noncopyable_function<endpoint_dc_rack(inet_address)>;

} // namespace locator
