/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"
#include <seastar/http/api_docs.hh>

namespace api {

void set_config(std::shared_ptr<api_registry_builder20> rb, http_context& ctx, routes& r);
}
