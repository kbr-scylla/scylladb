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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <memory>

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class ks_prop_defs;

class alter_keyspace_statement : public schema_altering_statement {
    sstring _name;
    ::shared_ptr<ks_prop_defs> _attrs;

public:
    alter_keyspace_statement(sstring name, ::shared_ptr<ks_prop_defs> attrs);

    const sstring& keyspace() const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;
    void validate(query_processor& qp, const service::client_state& state) const override;
    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_processor& qp) const override;
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    virtual future<::shared_ptr<messages::result_message>> execute(query_processor& qp, service::query_state& state, const query_options& options) const override;
};

}
}
