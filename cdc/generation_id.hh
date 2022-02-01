/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <variant>

#include "db_clock.hh"
#include "utils/UUID.hh"

namespace cdc {

struct generation_id_v1 {
    db_clock::time_point ts;
};

struct generation_id_v2 {
    db_clock::time_point ts;
    utils::UUID id;
};

using generation_id = std::variant<generation_id_v1, generation_id_v2>;

std::ostream& operator<<(std::ostream&, const generation_id&);
bool operator==(const generation_id&, const generation_id&);
db_clock::time_point get_ts(const generation_id&);

} // namespace cdc
