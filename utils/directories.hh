/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <set>
#include <vector>
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
    class set {
    public:
        void add(fs::path path);
        void add(sstring path);
        void add(std::vector<sstring> path);
        void add_sharded(sstring path);

        const std::set<fs::path> get_paths() const {
            return _paths;
        }

    private:
        std::set<fs::path> _paths;
    };

    directories(bool developer_mode);
    future<> create_and_verify(set dir_set);
    static future<> verify_owner_and_mode(std::filesystem::path path);
private:
    bool _developer_mode;
    std::vector<file_lock> _locks;
};

} // namespace utils
