/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

future<file> make_in_memory_mirror_file(file primary, sstring name, bool check_integrity = false, const io_priority_class& pc = default_priority_class());

future<> remove_mirrored_file(sstring path);
future<> rename_mirrored_file(sstring oldpath, sstring newpath);
future<> link_mirrored_file(sstring oldpath, sstring newpath);
