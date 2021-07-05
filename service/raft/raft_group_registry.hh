/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "message/messaging_service_fwd.hh"
#include "gms/inet_address.hh"
#include "raft/raft.hh"
#include "raft/server.hh"
#include "service/raft/raft_address_map.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace gms {

class gossiper;

} // namespace gms

namespace service {

class raft_rpc;
class raft_gossip_failure_detector;

struct raft_group_not_found: public raft::error {
    raft::group_id gid;
    raft_group_not_found(raft::group_id gid_arg)
            : raft::error(format("Raft group {} not found", gid_arg)), gid(gid_arg)
    {}
};

// This class is responsible for creating, storing and accessing raft servers.
// It also manages the raft rpc verbs initialization.
//
// `peering_sharded_service` inheritance is used to forward requests
// to the owning shard for a given raft group_id.
class raft_group_registry : public seastar::peering_sharded_service<raft_group_registry> {
public:
    using ticker_type = seastar::timer<lowres_clock>;
    // TODO: should be configurable.
    static constexpr ticker_type::duration tick_interval = std::chrono::milliseconds(100);
private:
    netw::messaging_service& _ms;
    gms::gossiper& _gossiper;
    sharded<cql3::query_processor>& _qp;
    // Shard-local failure detector instance shared among all raft groups
    shared_ptr<raft_gossip_failure_detector> _fd;

    struct server_for_group {
        raft::group_id gid;
        std::unique_ptr<raft::server> server;
        std::unique_ptr<ticker_type> ticker;
        raft_rpc& rpc;
    };
    // Raft servers along with the corresponding timers to tick each instance.
    // Currently ticking every 100ms.
    std::unordered_map<raft::group_id, server_for_group> _servers;
    // inet_address:es for remote raft servers known to us
    raft_address_map<> _srv_address_mappings;

    void init_rpc_verbs();
    seastar::future<> uninit_rpc_verbs();
    seastar::future<> stop_servers();

    server_for_group create_server_for_group(raft::group_id id);

    server_for_group& get_server_for_group(raft::group_id id);
public:

    raft_group_registry(netw::messaging_service& ms, gms::gossiper& gs, sharded<cql3::query_processor>& qp);
    // To integrate Raft service into the boot procedure, the
    // initialization is split into 2 steps:
    // - in sharded::start(), we construct an instance of
    // raft_group_registry on each shard. The RPC is not available
    // yet and no groups are created. The created object is
    // useless but it won't crash on use.
    // - in init(), which must be invoked explicitly on each
    // shard after the query processor and database have started,
    // we boot all existing groups from the local system tables
    // and start RPC
    seastar::future<> init();
    // Must be invoked explicitly on each shard to stop this service.
    seastar::future<> uninit();

    raft_rpc& get_rpc(raft::group_id gid);

    // Find server for group by group id. Throws exception if
    // there is no such group.
    raft::server& get_server(raft::group_id gid);

    // Start raft server instance, store in the map of raft servers and
    // arm the associated timer to tick the server.
    future<> start_server_for_group(server_for_group grp);
    unsigned shard_for_group(const raft::group_id& gid) const;

    // Map raft server_id to inet_address to be consumed by `messaging_service`
    gms::inet_address get_inet_address(raft::server_id id) const;
    // Update inet_address mapping for a raft server with a given id.
    // In case a mapping exists for a given id, it should be equal to the supplied `addr`
    // otherwise the function will throw.
    void update_address_mapping(raft::server_id id, gms::inet_address addr, bool expiring);
    // Remove inet_address mapping for a raft server
    void remove_address_mapping(raft::server_id);
};

} // end of namespace service
