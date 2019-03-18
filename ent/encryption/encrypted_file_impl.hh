/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/file.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"

namespace encryption {

class symmetric_key;

shared_ptr<file_impl> make_encrypted_file(file, ::shared_ptr<symmetric_key>);

using get_key_func = std::function<future<::shared_ptr<symmetric_key>>()>;

shared_ptr<file_impl> make_delayed_encrypted_file(file, size_t, get_key_func);
}
