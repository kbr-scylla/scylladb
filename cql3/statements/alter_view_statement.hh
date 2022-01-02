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

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {

class query_processor;
class cf_name;

namespace statements {

/** An <code>ALTER MATERIALIZED VIEW</code> parsed from a CQL query statement. */
class alter_view_statement : public schema_altering_statement {
private:
    std::optional<cf_prop_defs> _properties;
    view_ptr prepare_view(data_dictionary::database db) const;
public:
    alter_view_statement(cf_name view_name, std::optional<cf_prop_defs> properties);

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual void validate(query_processor&, const service::client_state& state) const override;


    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_processor& qp) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}
}
