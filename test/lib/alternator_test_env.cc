/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "test/lib/alternator_test_env.hh"
#include "alternator/rmw_operation.hh"
#include "replica/database.hh"
#include <seastar/core/coroutine.hh>

#include "service/storage_proxy.hh"

future<> alternator_test_env::start(std::string_view isolation_level) {
    smp_service_group_config c;
    c.max_nonlocal_requests = 5000;
    smp_service_group ssg = co_await create_smp_service_group(c);

    co_await _sdks.start(std::ref(_qp), std::ref(_mm), std::ref(_proxy));
    co_await _cdc_metadata.start();
    co_await _executor.start(
            std::ref(_gossiper),
            std::ref(_proxy),
            std::ref(_mm),
            // parameters below are only touched by alternator streams;
            //  not really interesting for this use case
            std::ref(_sdks),
            std::ref(_cdc_metadata),
            // end-of-streams-parameters
            ssg);
    try {
        alternator::rmw_operation::set_default_write_isolation(isolation_level);
    } catch (const std::runtime_error& e) {
        std::cout << e.what() << std::endl;
        throw;
    }
}

future<> alternator_test_env::stop() {
    co_await _executor.stop();
    co_await _cdc_metadata.stop();
    co_await _sdks.stop();
}

future<> alternator_test_env::flush_memtables() {
    return _proxy.local().get_db().invoke_on_all(&replica::database::flush_all_memtables);
}
