/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <tuple>
#include <optional>
#include <seastar/core/seastar.hh>

namespace db {
    class commitlog_file_extension {
    public:
        virtual ~commitlog_file_extension() {}
        virtual seastar::future<seastar::file> wrap_file(const seastar::sstring& filename,
            seastar::file, seastar::open_flags flags) = 0;
        virtual seastar::future<> before_delete(const seastar::sstring& filename) = 0;
    };
}

