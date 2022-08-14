/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "raft/raft.hh"
#include "gms/inet_address_serializer.hh"

#include "idl/frozen_schema.idl.hh"
#include "idl/uuid.idl.hh"
#include "idl/raft_storage.idl.hh"

namespace service {

struct schema_change {
    std::vector<canonical_mutation> mutations;
};

struct group0_command {
    std::variant<service::schema_change> change;
    canonical_mutation history_append;

    std::optional<utils::UUID> prev_state_id;
    utils::UUID new_state_id;

    gms::inet_address creator_addr;
    raft::server_id creator_id;
};

} // namespace service
