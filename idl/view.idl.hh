/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

namespace db {
namespace view {
class update_backlog {
    size_t current;
    size_t max;
};
}
}
