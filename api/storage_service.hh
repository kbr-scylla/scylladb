/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"

namespace api {

void set_storage_service(http_context& ctx, routes& r);
void set_snapshot(http_context& ctx, routes& r);

}
