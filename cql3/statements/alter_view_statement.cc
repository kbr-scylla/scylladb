/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/alter_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "validation.hh"
#include "view_info.hh"
#include "db/extensions.hh"
#include "database.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

alter_view_statement::alter_view_statement(cf_name view_name, std::optional<cf_prop_defs> properties)
        : schema_altering_statement{std::move(view_name)}
        , _properties{std::move(properties)}
{
}

future<> alter_view_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    try {
        const database& db = proxy.local_db();
        auto&& s = db.find_schema(keyspace(), column_family());
        if (s->is_view())  {
            return state.has_column_family_access(db, keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

void alter_view_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    // validated in prepare_schema_mutations()
}

view_ptr alter_view_statement::prepare_view(database& db) const {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    if (!schema->is_view()) {
        throw exceptions::invalid_request_exception("Cannot use ALTER MATERIALIZED VIEW on Table");
    }

    if (!_properties) {
        throw exceptions::invalid_request_exception("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");
    }

    auto schema_extensions = _properties->make_schema_extensions(db.extensions());
    _properties->validate(db, schema_extensions);

    auto builder = schema_builder(schema);
    _properties->apply_to_builder(builder, std::move(schema_extensions));

    if (builder.get_gc_grace_seconds() == 0) {
        throw exceptions::invalid_request_exception(
                "Cannot alter gc_grace_seconds of a materialized view to 0, since this "
                "value is used to TTL undelivered updates. Setting gc_grace_seconds too "
                "low might cause undelivered updates to expire before being replayed.");
    }

    if (builder.default_time_to_live().count() > 0) {
        throw exceptions::invalid_request_exception(
                "Cannot set or alter default_time_to_live for a materialized view. "
                "Data in a materialized view always expire at the same time than "
                "the corresponding data in the parent table.");
    }

    return view_ptr(builder.build());
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> alter_view_statement::prepare_schema_mutations(query_processor& qp) const {
    auto m = co_await qp.get_migration_manager().prepare_view_update_announcement(prepare_view(qp.db()));

    using namespace cql_transport;
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
alter_view_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<alter_view_statement>(*this));
}

}

}
