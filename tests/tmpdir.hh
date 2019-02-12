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

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
class tmpdir {
    seastar::compat::filesystem::path _path;

private:
    void remove() {
        if (!_path.empty()) {
            seastar::compat::filesystem::remove_all(_path);
        }
    }

public:
    tmpdir()
     : _path(seastar::compat::filesystem::temp_directory_path() /
             fmt::format(FMT_STRING("scylla-{}"), utils::make_random_uuid())) {
        seastar::compat::filesystem::create_directories(_path);
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

    const seastar::compat::filesystem::path& path() const noexcept { return _path; }
};
