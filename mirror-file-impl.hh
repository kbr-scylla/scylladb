/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

future<> remove_mirrored_file(sstring path);
future<> rename_mirrored_file(sstring oldpath, sstring newpath);
future<> link_mirrored_file(sstring oldpath, sstring newpath);
