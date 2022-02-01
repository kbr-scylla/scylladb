/*
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "seastarx.hh"

namespace replica {
class database;
}

namespace db {

class commitlog;

class commitlog_replayer {
public:
    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    static future<commitlog_replayer> create_replayer(seastar::sharded<replica::database>&);

    future<> recover(std::vector<sstring> files, sstring fname_prefix);
    future<> recover(sstring file, sstring fname_prefix);

private:
    commitlog_replayer(seastar::sharded<replica::database>&);

    class impl;
    std::unique_ptr<impl> _impl;
};

}
