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

// Raft state machine implementation for managing schema changes.
// NOTE: schema raft server is always instantiated on shard 0.
class schema_raft_state_machine : public raft::state_machine {
public:
    // schema raft server always belongs to a special pre-defined raft group with id = UUID(0, 0)
    // TODO: 0-raft group id should be unique for each scylla cluster
    static constexpr raft::group_id gid;

    future<> apply(std::vector<raft::command_cref> command) override;
    future<raft::snapshot_id> take_snapshot() override;
    void drop_snapshot(raft::snapshot_id id) override;
    future<> load_snapshot(raft::snapshot_id id) override;
    future<> abort() override;
};
