/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include "service/raft/schema_raft_state_machine.hh"

namespace service {

future<> schema_raft_state_machine::apply(std::vector<raft::command_cref> command) {
    return make_ready_future<>();
}

future<raft::snapshot_id> schema_raft_state_machine::take_snapshot() {
    return make_ready_future<raft::snapshot_id>(raft::snapshot_id::create_random_id());
}

void schema_raft_state_machine::drop_snapshot(raft::snapshot_id id) {
    (void) id;
}

future<> schema_raft_state_machine::load_snapshot(raft::snapshot_id id) {
    return make_ready_future<>();
}

future<> schema_raft_state_machine::abort() {
    return make_ready_future<>();
}

} // end of namespace service
