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

namespace api {

void set_stream_manager(http_context& ctx, routes& r, sharded<streaming::stream_manager>& sm);
void unset_stream_manager(http_context& ctx, routes& r);

}
