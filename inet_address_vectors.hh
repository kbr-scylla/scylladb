/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "gms/inet_address.hh"
#include "utils/small_vector.hh"

using inet_address_vector_replica_set = utils::small_vector<gms::inet_address, 3>;

using inet_address_vector_topology_change = utils::small_vector<gms::inet_address, 1>;
