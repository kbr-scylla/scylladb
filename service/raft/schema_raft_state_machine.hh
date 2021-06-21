/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include "raft/raft.hh"
#include "utils/UUID_gen.hh"

// Raft state machine implementation for managing schema changes.
// NOTE: schema raft server is always instantiated on shard 0.
class schema_raft_state_machine : public raft::state_machine {
public:
    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> abort() override;
};
