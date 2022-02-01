/*

 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "utils/serialization.hh"
#include "gms/gossip_digest.hh"
#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "utils/chunked_vector.hh"

namespace gms {

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
class gossip_digest_ack {
private:
    using inet_address = gms::inet_address;
    utils::chunked_vector<gossip_digest> _digests;
    std::map<inet_address, endpoint_state> _map;
public:
    gossip_digest_ack() {
    }

    gossip_digest_ack(utils::chunked_vector<gossip_digest> d, std::map<inet_address, endpoint_state> m)
        : _digests(std::move(d))
        , _map(std::move(m)) {
    }

    const utils::chunked_vector<gossip_digest>& get_gossip_digest_list() const {
        return _digests;
    }

    std::map<inet_address, endpoint_state>& get_endpoint_state_map() {
        return _map;
    }

    const std::map<inet_address, endpoint_state>& get_endpoint_state_map() const {
        return _map;
    }

    friend std::ostream& operator<<(std::ostream& os, const gossip_digest_ack& ack);
};

}
