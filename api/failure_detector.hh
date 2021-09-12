/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"

namespace gms {

class gossiper;

}

namespace api {

void set_failure_detector(http_context& ctx, routes& r, gms::gossiper& g);

}
