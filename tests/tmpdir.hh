/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <fmt/format.h>

#include <seastar/util/std-compat.hh>

#include "utils/UUID.hh"

namespace fs = std::filesystem;

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
class tmpdir {
    fs::path _path;

private:
    void remove() {
        if (!_path.empty()) {
            fs::remove_all(_path);
        }
    }

public:
    tmpdir()
     : _path(fs::temp_directory_path() /
             fs::path(fmt::format(FMT_STRING("scylla-{}"), utils::make_random_uuid()))) {
        fs::create_directories(_path);
    }

    tmpdir(tmpdir&& other) noexcept : _path(std::exchange(other._path, {})) { }
    tmpdir(const tmpdir&) = delete;
    void operator=(tmpdir&& other) noexcept {
        remove();
        _path = std::exchange(other._path, {});
    }
    void operator=(const tmpdir&) = delete;

    ~tmpdir() {
        remove();
    }

    const fs::path& path() const noexcept { return _path; }
};
