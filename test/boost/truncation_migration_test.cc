/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <string_view>
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "utils/joinpoint.hh"

SEASTAR_TEST_CASE(test_truncation_record_migration) {
    cql_test_config cfg;

    cfg.disabled_features = { "TRUNCATION_TABLE" };

    return do_with_cql_env_thread([](cql_test_env& e) {
        e.execute_cql("CREATE TABLE test (a int, b int, PRIMARY KEY (a))").get();
        e.execute_cql("INSERT INTO test (a, b) VALUES (1, 100);").get();

        assert_that(e.execute_cql("SELECT * FROM test").get0())
            .is_rows().with_size(1);

        assert_that(e.execute_cql("SELECT * FROM system.truncated").get0())
            .is_rows().is_empty();
        assert_that(e.execute_cql("SELECT truncated_at FROM system.local WHERE key = 'local'").get0())
            .is_rows().is_null();

        // Do a truncation
        // Cannot do via cql, because we don't have an actual functioning rpc active.

        do_with(utils::make_joinpoint([] { return db_clock::now();}), [](auto& tsf) {
            return service::get_storage_proxy().invoke_on_all([&tsf](service::storage_proxy& sp) {
                return sp.get_db().local().truncate("ks", "test", [&tsf] { return tsf.value(); });
            });
        }).get();

        assert_that(e.execute_cql("SELECT * FROM test").get0())
            .is_rows().is_empty();


        assert_that(e.execute_cql("SELECT * FROM system.truncated").get0())
            .is_rows().is_not_empty();
        // should also have created legacy record
        assert_that(e.execute_cql("SELECT truncated_at FROM system.local WHERE key = 'local'").get0())
            .is_rows().is_not_null();

        // Now enable truncation_table feature. Should remove the
        // legacy records.

        service::get_storage_service().invoke_on_all([] (service::storage_service& ss) {
            ss.features().cluster_supports_truncation_table().enable();
        }).get();

        db::system_keyspace::wait_for_truncation_record_migration_complete().get();

        assert_that(e.execute_cql("SELECT truncated_at FROM system.local WHERE key = 'local'").get0())
            .is_rows().is_null();

    }, cfg);
}

