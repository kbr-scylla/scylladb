/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include "service/raft/schema_raft_state_machine.hh"

future<> schema_raft_state_machine::apply(std::vector<raft::command_cref> command) {
    throw std::runtime_error("Not implemented");
}

future<raft::snapshot_id> schema_raft_state_machine::take_snapshot() {
    throw std::runtime_error("Not implemented");
}

void schema_raft_state_machine::drop_snapshot(raft::snapshot_id id) {
    throw std::runtime_error("Not implemented");
}

future<> schema_raft_state_machine::load_snapshot(raft::snapshot_id id) {
    throw std::runtime_error("Not implemented");
}

future<> schema_raft_state_machine::abort() {
    return make_ready_future<>();
}
