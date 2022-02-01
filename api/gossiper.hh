/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "api.hh"

namespace gms {

class gossiper;

}

namespace api {

void set_gossiper(http_context& ctx, routes& r, gms::gossiper& g);

}
