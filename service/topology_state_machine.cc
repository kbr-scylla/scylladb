/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "topology_state_machine.hh"

namespace service {

logging::logger tsmlogger("topology_state_machine");

const std::pair<const raft::server_id, replica_state>* topology::find(raft::server_id id) {
    auto it = normal_nodes.find(id);
    if (it != normal_nodes.end()) {
        return &*it;
    }
    it = transition_nodes.find(id);
    if (it != transition_nodes.end()) {
        return &*it;
    }
    it = new_nodes.find(id);
    if (it != new_nodes.end()) {
        return &*it;
    }
    return nullptr;
}

bool topology::contains(raft::server_id id) {
    return normal_nodes.contains(id) ||
           transition_nodes.contains(id) ||
           new_nodes.contains(id) ||
           left_nodes.contains(id);
}

static std::unordered_map<ring_slice::replication_state, sstring> replication_state_to_name_map = {
    {ring_slice::replication_state::write_both_read_old, "write both read old"},
    {ring_slice::replication_state::write_both_read_new, "write both read new"},
    {ring_slice::replication_state::owner, "owner"},
};

std::ostream& operator<<(std::ostream& os, ring_slice::replication_state s) {
    auto it = replication_state_to_name_map.find(s);
    if (it == replication_state_to_name_map.end()) {
        on_internal_error(tsmlogger, "cannot print replication_state");
    }
    return os << it->second;
}

ring_slice::replication_state replication_state_from_string(const sstring& s) {
    for (auto&& e : replication_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    on_internal_error(tsmlogger, format("cannot map name {} to replication_state", s));
}

static std::unordered_map<node_state, sstring> node_state_to_name_map = {
    {node_state::bootstrapping, "bootstrapping"},
    {node_state::decommissioning, "decommissioning"},
    {node_state::removing, "removing"},
    {node_state::normal, "normal"},
    {node_state::left, "left"},
    {node_state::replacing, "replacing"},
    {node_state::rebuilding, "rebuilding"},
    {node_state::none, "none"}
};

std::ostream& operator<<(std::ostream& os, node_state s) {
    auto it = node_state_to_name_map.find(s);
    if (it == node_state_to_name_map.end()) {
        on_internal_error(tsmlogger, "cannot print node_state");
    }
    return os << it->second;
}

node_state node_state_from_string(const sstring& s) {
    for (auto&& e : node_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    on_internal_error(tsmlogger, format("cannot map name {} to node_state", s));
}

static std::unordered_map<topology_request, sstring> topology_request_to_name_map = {
    {topology_request::join, "join"},
    {topology_request::leave, "leave"},
    {topology_request::remove, "remove"},
    {topology_request::replace, "replace"},
    {topology_request::rebuild, "rebuild"}
};

std::ostream& operator<<(std::ostream& os, const topology_request& req) {
    os << topology_request_to_name_map[req];
    return os;
}

topology_request topology_request_from_string(const sstring& s) {
    for (auto&& e : topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to topology_request", s));
}

std::ostream& operator<<(std::ostream& os, const raft_topology_cmd::command& cmd) {
    switch (cmd) {
        case raft_topology_cmd::command::barrier:
            os << "barrier";
            break;
        case raft_topology_cmd::command::stream_ranges:
            os << "stream_ranges";
            break;
        case raft_topology_cmd::command::fence_old_reads:
            os << "fence_old_reads";
            break;
    }
    return os;
}
}
