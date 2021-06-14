
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "locator/rack_inferring_snitch.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, rack_inferring_snitch>;
static registry registrator1("org.apache.cassandra.locator.RackInferringSnitch");
static registry registrator2("RackInferringSnitch");
}
