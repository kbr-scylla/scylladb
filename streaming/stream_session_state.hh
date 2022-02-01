/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <ostream>

namespace streaming {

enum class stream_session_state {
    INITIALIZED,
    PREPARING,
    STREAMING,
    WAIT_COMPLETE,
    COMPLETE,
    FAILED,
};

std::ostream& operator<<(std::ostream& os, const stream_session_state& s);

} // namespace
