/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>

#include "log.hh"
#include "seastarx.hh"

namespace service {
class migration_manager;
}

namespace cql3 {
class query_processor;
}

namespace auth {

namespace meta {

extern const sstring DEFAULT_SUPERUSER_NAME;
extern const sstring AUTH_KS;
extern const sstring USERS_CF;
extern const sstring AUTH_PACKAGE_NAME;

}

template <class Task>
future<> once_among_shards(Task&& f) {
    if (engine().cpu_id() == 0u) {
        return f();
    }

    return make_ready_future<>();
}

inline future<> delay_until_system_ready(seastar::abort_source& as) {
    using namespace std::chrono_literals;
    return sleep_abortable(10s, as);
}

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func);

future<> create_metadata_table_if_missing(
        const sstring& table_name,
        cql3::query_processor&,
        const sstring& cql,
        ::service::migration_manager&);

}
