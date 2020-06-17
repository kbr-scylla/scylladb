/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <unordered_set>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include "utils/file_lock.hh"

using namespace seastar;

namespace db {
class config;
}

namespace utils {

class directories {
public:
    future<> init(db::config& cfg, bool hinted_handoff_enabled);
private:
    future<> touch_and_lock(fs::path path);
    std::vector<file_lock> _locks;
};

} // namespace utils
