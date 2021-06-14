
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "locator/simple_snitch.hh"
#include "utils/class_registrator.hh"

namespace locator {
using registry = class_registrator<i_endpoint_snitch, simple_snitch>;
static registry registrator1("org.apache.cassandra.locator.SimpleSnitch");
static registry registrator2("SimpleSnitch");
}
