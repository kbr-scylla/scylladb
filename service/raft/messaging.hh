/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */
#pragma once
#include "raft/raft.hh"
/////////////////////////////////////////
// Discovery RPC supporting message types

namespace service {

// Used in a bootstrapped Scylla cluster, provides group  0
// identifier and the current group leader address.
struct group0_info {
    raft::group_id group0_id;
    raft::server_address addr;
    bool operator==(const group0_info& rhs) const {
        return rhs.group0_id == group0_id && rhs.addr == addr;
    }
};

// If the peer has no cluster discovery running, it returns
// std::monostate, which means the caller needs to retry
// contacting this server after a pause. Otherwise it returns
// its leader data or a list of peers.
struct group0_peer_exchange {
    std::variant<std::monostate, group0_info, std::vector<raft::server_address>> info;
};

/////////////////////////////////////////
} // end of namespace service

