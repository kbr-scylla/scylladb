
/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/bool_class.hh>
#include "seastarx.hh"

namespace db {

using commitlog_force_sync = bool_class<class force_sync_tag>;

}
