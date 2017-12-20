/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "auth/common.hh"

#include <seastar/core/shared_ptr.hh>

#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"

namespace auth {

namespace meta {

const sstring DEFAULT_SUPERUSER_NAME("cassandra");
const sstring AUTH_KS("system_auth");
const sstring USERS_CF("users");
const sstring AUTH_PACKAGE_NAME("org.apache.cassandra.auth.");

}

static logging::logger auth_log("auth");

// Func must support being invoked more than once.
future<> do_after_system_ready(seastar::abort_source& as, seastar::noncopyable_function<future<>()> func) {
    using namespace std::chrono_literals;
    struct empty_state { };
    return delay_until_system_ready(as).then([&as, func = std::move(func)] () mutable {
        return exponential_backoff_retry::do_until_value(1s, 1min, as, [func = std::move(func)] {
            return func().then_wrapped([] (auto&& f) -> stdx::optional<empty_state> {
                if (f.failed()) {
                    auth_log.warn("Auth task failed with error, rescheduling: {}", f.get_exception());
                    return { };
                }
                return { empty_state() };
            });
        });
    }).discard_result();
}

future<> create_metadata_table_if_missing(
        const sstring& table_name,
        cql3::query_processor& qp,
        const sstring& cql,
        ::service::migration_manager& mm) {
    auto& db = qp.db().local();

    if (db.has_schema(meta::AUTH_KS, table_name)) {
        return make_ready_future<>();
    }

    auto parsed_statement = static_pointer_cast<cql3::statements::raw::cf_statement>(
            cql3::query_processor::parse_statement(cql));

    parsed_statement->prepare_keyspace(meta::AUTH_KS);

    auto statement = static_pointer_cast<cql3::statements::create_table_statement>(
            parsed_statement->prepare(db, qp.get_cql_stats())->statement);

    const auto schema = statement->get_cf_meta_data();
    const auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    return mm.announce_new_column_family(b.build(), false);
}

}
