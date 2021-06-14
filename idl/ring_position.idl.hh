/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace dht {
class ring_position {
    enum class token_bound:int8_t {start = -1, end = 1};
    dht::token token();
    dht::ring_position::token_bound bound();
    std::optional<partition_key> key();
};
}
