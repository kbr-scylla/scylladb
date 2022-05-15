/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "prepare_response.hh"

namespace service {

namespace paxos {

std::ostream& operator<<(std::ostream& os, const promise& promise) {
    os << "prepare_promise(";
    promise.most_recent_commit ? os << *promise.most_recent_commit : os << "empty";
    os << ", ";
    promise.accepted_proposal ? os << *promise.accepted_proposal : os << "empty";
    return os << ")";
}

} // end of namespace "paxos"
} // end of namespace "service"
