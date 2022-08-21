/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "shared_sstable.hh"
#include "utils/UUID.hh"

namespace sstables {

using run_id = utils::tagged_uuid<struct run_id_tag>;

} // namespace sstables
