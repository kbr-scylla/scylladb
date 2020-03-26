/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

#include "utils/disk-error-handler.hh"

#include "seastarx.hh"

namespace sstables {

future<> remove_by_toc_name(sstring sstable_toc_name, const io_error_handler& error_handler = sstable_write_error_handler);

}


