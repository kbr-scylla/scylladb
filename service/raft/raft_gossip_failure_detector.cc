/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "service/raft/raft_gossip_failure_detector.hh"
#include "service/raft/raft_services.hh"
#include "gms/gossiper.hh"

raft_gossip_failure_detector::raft_gossip_failure_detector(gms::gossiper& gs, raft_services& raft_svcs)
    : _gossip(gs), _raft_services(raft_svcs)
{}

bool raft_gossip_failure_detector::is_alive(raft::server_id server) {
    return _gossip.is_alive(_raft_services.get_inet_address(server));
}
