/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "streaming/stream_summary.hh"
#include "types.hh"
#include "utils/serialization.hh"

namespace streaming {

std::ostream& operator<<(std::ostream& os, const stream_summary& x) {
    os << "[ cf_id=" << x.cf_id << " ]";
    return os;
}

} // namespace streaming
