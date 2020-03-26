/*
 * Copyright (C) 2015 ScyllaDB.
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once
#include <vector>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace dht {
struct endpoint_details {
    sstring _host;
    sstring _datacenter;
    sstring _rack;
};

struct token_range_endpoints {
    sstring _start_token;
    sstring _end_token;
    std::vector<sstring> _endpoints;
    std::vector<sstring> _rpc_endpoints;
    std::vector<endpoint_details> _endpoint_details;
};

}
