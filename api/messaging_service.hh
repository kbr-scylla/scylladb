/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace netw { class messaging_service; }

namespace api {

void set_messaging_service(http_context& ctx, routes& r, sharded<netw::messaging_service>& ms);
void unset_messaging_service(http_context& ctx, routes& r);

}
