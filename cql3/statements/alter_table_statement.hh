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
 * Copyright 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/cql3_type.hh"
#include "cql3/column_identifier.hh"
#include "database_fwd.hh"

namespace cql3 {

class query_processor;

namespace statements {

class alter_table_statement : public schema_altering_statement {
public:
    enum class type {
        add,
        alter,
        drop,
        opts,
        rename,
    };
    using renames_type = std::vector<std::pair<shared_ptr<column_identifier::raw>,
                                               shared_ptr<column_identifier::raw>>>;
    struct column_change {
        shared_ptr<column_identifier::raw> name;
        shared_ptr<cql3_type::raw> validator = nullptr;
        bool is_static = false;
    };
private:
    const type _type;
    const std::vector<column_change> _column_changes;
    const std::optional<cf_prop_defs> _properties;
    const renames_type _renames;
public:
    alter_table_statement(cf_name name,
                          type t,
                          std::vector<column_change> column_changes,
                          std::optional<cf_prop_defs> properties,
                          renames_type renames);

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    virtual future<::shared_ptr<messages::result_message>> execute(query_processor& qp, service::query_state& state, const query_options& options) const override;

    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_processor& qp) const override;
private:
    void add_column(const schema& schema, const table& cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    void alter_column(const schema& schema, const table& cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    void drop_column(const schema& schema, const table& cf, schema_builder& cfm, std::vector<view_ptr>& view_updates, const column_identifier& column_name, const cql3_type validator, const column_definition* def, bool is_static) const;
    std::pair<schema_builder, std::vector<view_ptr>> prepare_schema_update(database& db) const;
};

}

}
