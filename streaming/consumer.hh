/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
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

std::function<future<>(flat_mutation_reader)> make_streaming_consumer(sstring origin,
    sharded<replica::database>& db,
    sharded<db::system_distributed_keyspace>& sys_dist_ks,
    sharded<db::view::view_update_generator>& vug,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy);

}
