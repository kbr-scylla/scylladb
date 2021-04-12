/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <variant>

#include "db_clock.hh"

namespace cdc {

struct generation_id {
    db_clock::time_point ts;
};

std::ostream& operator<<(std::ostream&, const generation_id&);
bool operator==(const generation_id&, const generation_id&);
db_clock::time_point get_ts(const generation_id&);

} // namespace cdc
