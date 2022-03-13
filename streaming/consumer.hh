/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "sstables/sstable_set.hh"
#include "streaming/stream_reason.hh"

namespace replica {
class database;
}

namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
}
}

namespace streaming {

std::function<future<>(flat_mutation_reader_v2)> make_streaming_consumer(sstring origin,
    sharded<replica::database>& db,
    sharded<db::system_distributed_keyspace>& sys_dist_ks,
    sharded<db::view::view_update_generator>& vug,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy);

}
