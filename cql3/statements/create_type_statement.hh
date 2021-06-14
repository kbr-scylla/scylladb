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
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/ut_name.hh"
#include "database_fwd.hh"

namespace cql3 {

class query_processor;

namespace statements {

class create_type_statement : public schema_altering_statement {
    ut_name _name;
    std::vector<::shared_ptr<column_identifier>> _column_names;
    std::vector<::shared_ptr<cql3_type::raw>> _column_types;
    bool _if_not_exists;
public:
    create_type_statement(const ut_name& name, bool if_not_exists);

    virtual void prepare_keyspace(const service::client_state& state) override;

    void add_definition(::shared_ptr<column_identifier> name, ::shared_ptr<cql3_type::raw> type);

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    virtual const sstring& keyspace() const override;

    virtual future<shared_ptr<cql_transport::event::schema_change>> announce_migration(query_processor& qp) const override;

    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;

    static void check_for_duplicate_names(user_type type);
private:
    bool type_exists_in(::keyspace& ks) const;
    user_type create_type(database& db) const;
};

}

}
