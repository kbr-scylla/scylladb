/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"
#include <seastar/http/api_docs.hh>

namespace api {

void set_config(std::shared_ptr<api_registry_builder20> rb, http_context& ctx, routes& r, const db::config& cfg);
}
