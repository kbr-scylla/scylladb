/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "../../reader_permit.hh"
#include "query_class_config.hh"

class reader_concurrency_semaphore;

namespace tests {

reader_concurrency_semaphore& semaphore();

reader_permit make_permit();

query::query_class_config make_query_class_config();

} // namespace tests
