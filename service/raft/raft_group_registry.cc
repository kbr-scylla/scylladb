/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_gossip_failure_detector.hh"
#include "message/messaging_service.hh"
#include "serializer_impl.hh"


#include <seastar/core/coroutine.hh>

namespace service {

logging::logger rslog("raft_group_registry");

raft_group_registry::raft_group_registry(bool is_enabled, netw::messaging_service& ms, gms::gossiper& gossiper)
    : _is_enabled(is_enabled), _ms(ms), _fd(make_shared<raft_gossip_failure_detector>(gossiper, _srv_address_mappings))
{
}

void raft_group_registry::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            const raft::group_id& gid, raft::server_id from, raft::server_id dst, auto handler) {
        return container().invoke_on(shard_for_group(gid),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, dst, gid, handler] (raft_group_registry& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(gid);
            // The address learnt from a probably unknown server should
            // eventually expire
            self._srv_address_mappings.set(from, std::move(addr), true);
            // Execute the actual message handling code
            return handler(rpc);
        });
    };

    _ms.register_raft_send_snapshot([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::install_snapshot snp) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, snp = std::move(snp)] (raft_rpc& rpc) mutable {
            return rpc.apply_snapshot(std::move(from), std::move(snp));
        });
    });

    _ms.register_raft_append_entries([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
           raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_request append_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, append_request = std::move(append_request)] (raft_rpc& rpc) mutable {
            rpc.append_entries(std::move(from), std::move(append_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_append_entries_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_request([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_timeout_now([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::timeout_now timeout_now) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, timeout_now] (raft_rpc& rpc) mutable {
            rpc.timeout_now_request(std::move(from), std::move(timeout_now));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_read_quorum([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum read_quorum) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum] (raft_rpc& rpc) mutable {
            rpc.read_quorum_request(std::move(from), std::move(read_quorum));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_read_quorum_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum_reply read_quorum_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum_reply] (raft_rpc& rpc) mutable {
            rpc.read_quorum_reply(std::move(from), std::move(read_quorum_reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_execute_read_barrier_on_leader([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from] (raft_rpc& rpc) mutable {
            return rpc.execute_read_barrier(from);
        });
    });

    _ms.register_raft_add_entry([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::command cmd) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, cmd = std::move(cmd)] (raft_rpc& rpc) mutable {
            return rpc.execute_add_entry(from, std::move(cmd));
        });
    });

    _ms.register_raft_modify_config([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst,
            std::vector<raft::server_address> add, std::vector<raft::server_id> del) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst,
            [from, add = std::move(add), del = std::move(del)] (raft_rpc& rpc) mutable {

            return rpc.execute_modify_config(from, std::move(add), std::move(del));
        });
    });
}

future<> raft_group_registry::uninit_rpc_verbs() {
    return when_all_succeed(
        _ms.unregister_raft_send_snapshot(),
        _ms.unregister_raft_append_entries(),
        _ms.unregister_raft_append_entries_reply(),
        _ms.unregister_raft_vote_request(),
        _ms.unregister_raft_vote_reply(),
        _ms.unregister_raft_timeout_now(),
        _ms.unregister_raft_read_quorum(),
        _ms.unregister_raft_read_quorum_reply(),
        _ms.unregister_raft_execute_read_barrier_on_leader(),
        _ms.unregister_raft_add_entry(),
        _ms.unregister_raft_modify_config()
    ).discard_result();
}

future<> raft_group_registry::stop_servers() {
    std::vector<future<>> stop_futures;
    stop_futures.reserve(_servers.size());
    for (auto& entry : _servers) {
        stop_futures.emplace_back(entry.second.server->abort());
    }
    co_await when_all_succeed(stop_futures.begin(), stop_futures.end());
}

seastar::future<> raft_group_registry::start() {
    if (!_is_enabled) {
        co_return;
    }
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    co_return init_rpc_verbs();
}

seastar::future<> raft_group_registry::stop() {
    if (!_is_enabled) {
        co_return;
    }
    co_await when_all_succeed(
        _shutdown_gate.close(),
        uninit_rpc_verbs(),
        stop_servers()
    ).discard_result();
}

raft_server_for_group& raft_group_registry::server_for_group(raft::group_id gid) {
    auto it = _servers.find(gid);
    if (it == _servers.end()) {
        throw raft_group_not_found(gid);
    }
    return it->second;
}

raft_rpc& raft_group_registry::get_rpc(raft::group_id gid) {
    return server_for_group(gid).rpc;
}

raft::server& raft_group_registry::get_server(raft::group_id gid) {
    return *(server_for_group(gid).server);
}

raft::server& raft_group_registry::group0() {
    return *(server_for_group(*_group0_id).server);
}

future<> raft_group_registry::start_server_for_group(raft_server_for_group new_grp) {
    auto gid = new_grp.gid;
    auto [it, inserted] = _servers.emplace(std::move(gid), std::move(new_grp));

    if (!inserted) {
        on_internal_error(rslog, format("Attempt to add the second instance of raft server with the same gid={}", gid));
    }
    if (_servers.size() == 1 && this_shard_id() == 0) {
        _group0_id = gid;
    }
    auto& grp = it->second;
    try {
        // start the server instance prior to arming the ticker timer.
        // By the time the tick() is executed the server should already be initialized.
        co_await grp.server->start();
    } catch (...) {
        // remove server from the map to prevent calling `abort()` on a
        // non-started instance when `raft_group_registry::uninit` is called.
        _servers.erase(it);
        on_internal_error(rslog, std::current_exception());
    }

    grp.ticker->arm_periodic(raft_tick_interval);
}

unsigned raft_group_registry::shard_for_group(const raft::group_id& gid) const {
    return 0; // schema raft server is always owned by shard 0
}

} // end of namespace service
