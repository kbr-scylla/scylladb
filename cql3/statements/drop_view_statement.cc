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
 * Copyright 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/statements/drop_view_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "view_info.hh"
#include "database.hh"

namespace cql3 {

namespace statements {

drop_view_statement::drop_view_statement(cf_name view_name, bool if_exists)
    : schema_altering_statement{std::move(view_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_view_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const
{
    try {
        const database& db = proxy.local_db();
        auto&& s = db.find_schema(keyspace(), column_family());
        if (s->is_view()) {
            return state.has_column_family_access(db, keyspace(), s->view_info()->base_name(), auth::permission::ALTER);
        }
    } catch (const no_such_column_family& e) {
        // Will be validated afterwards.
    }
    return make_ready_future<>();
}

void drop_view_statement::validate(service::storage_proxy&, const service::client_state& state) const
{
    // validated in migration_manager::announce_view_drop()
}

future<shared_ptr<cql_transport::event::schema_change>> drop_view_statement::announce_migration(query_processor& qp) const
{
    return make_ready_future<>().then([this, &mm = qp.get_migration_manager()] {
        return mm.announce_view_drop(keyspace(), column_family());
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            using namespace cql_transport;

            return ::make_shared<event::schema_change>(event::schema_change::change_type::DROPPED,
                                                     event::schema_change::target_type::TABLE,
                                                     this->keyspace(),
                                                     this->column_family());
        } catch (const exceptions::configuration_exception& e) {
            if (_if_exists) {
                return ::shared_ptr<cql_transport::event::schema_change>();
            }
            throw e;
        }
    });
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_view_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<drop_view_statement>(*this));
}

}

}
