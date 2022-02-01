/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

namespace dht {
class ring_position {
    enum class token_bound:int8_t {start = -1, end = 1};
    dht::token token();
    dht::ring_position::token_bound bound();
    std::optional<partition_key> key();
};
}
