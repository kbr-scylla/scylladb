/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

namespace db {
struct replay_position {
    uint64_t id;
    uint32_t pos;
};
}
