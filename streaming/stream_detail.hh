/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "query-request.hh"
#include "schema_fwd.hh"
#include <vector>
#include "range.hh"
#include "dht/i_partitioner.hh"

namespace streaming {

struct stream_detail {
    table_id cf_id;
    stream_detail() = default;
    stream_detail(table_id cf_id_)
        : cf_id(std::move(cf_id_)) {
    }
};

} // namespace streaming
