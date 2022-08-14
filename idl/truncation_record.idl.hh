/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "idl/replay_position.idl.hh"

namespace db {

struct truncation_record {
    uint32_t magic;
    std::vector<db::replay_position> positions;
    db_clock::time_point time_stamp;
};

}
