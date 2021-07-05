
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

namespace service {

class raft_group_registry;

// Scylla-specific implementation of raft failure detector module.
//
// Currently uses gossiper as underlying implementation to test for `is_alive(gms::inet_address)`.
// Gets the mapping from server id to gms::inet_address from RPC module.
class raft_gossip_failure_detector : public raft::failure_detector {
    gms::gossiper& _gossip;
    raft_group_registry& _raft_gr;

public:
    raft_gossip_failure_detector(gms::gossiper& gs, raft_group_registry& raft_gr);

    bool is_alive(raft::server_id server) override;
};

} // end of namespace service
