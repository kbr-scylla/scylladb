/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

// Used to ensure that all .hh files build, as well as a place to put
// out-of-line implementations.

#include "locator/simple_snitch.hh"
#include "locator/rack_inferring_snitch.hh"
#include "locator/gossiping_property_file_snitch.hh"
