/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/simple-stream.hh>
#include "bytes_ostream.hh"

namespace utils {

using input_stream = seastar::memory_input_stream<bytes_ostream::fragment_iterator>;

}
