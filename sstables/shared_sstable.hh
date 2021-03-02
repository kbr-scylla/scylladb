/*
 * Copyright (C) 2017 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <utility>
#include <functional>
#include <unordered_set>
#include <seastar/core/shared_ptr.hh>

namespace sstables {

class sstable;

};

// Customize deleter so that lw_shared_ptr can work with an incomplete sstable class
namespace seastar {

template <>
struct lw_shared_ptr_deleter<sstables::sstable> {
    static void dispose(sstables::sstable* sst);
};

}

namespace sstables {

using shared_sstable = seastar::lw_shared_ptr<sstable>;

}


