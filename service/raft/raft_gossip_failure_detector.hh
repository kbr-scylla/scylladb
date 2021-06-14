
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

namespace gms {
class gossiper;
}

class raft_services;

// Scylla-specific implementation of raft failure detector module.
//
// Currently uses gossiper as underlying implementation to test for `is_alive(gms::inet_address)`.
// Gets the mapping from server id to gms::inet_address from RPC module.
class raft_gossip_failure_detector : public raft::failure_detector {
    gms::gossiper& _gossip;
    raft_services& _raft_services;

public:
    raft_gossip_failure_detector(gms::gossiper& gs, raft_services& raft_svcs);

    bool is_alive(raft::server_id server) override;
};
