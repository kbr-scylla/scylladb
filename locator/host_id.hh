/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "utils/UUID.hh"

namespace locator {

using host_id = utils::tagged_uuid<struct host_id_tag>;

}

