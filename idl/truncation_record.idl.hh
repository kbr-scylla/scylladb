/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace db {
namespace system_keyspace {
struct truncation_record {
    uint32_t magic;
    std::vector<db::replay_position> positions;
    db_clock::time_point time_stamp;
};
}
}
