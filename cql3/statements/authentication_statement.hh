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

#pragma once

#include "cql3/cql_statement.hh"
#include "prepared_statement.hh"
#include "raw/parsed_statement.hh"
#include "transport/messages_fwd.hh"

namespace cql3 {

namespace statements {

class authentication_statement : public raw::parsed_statement, public cql_statement_no_metadata, public ::enable_shared_from_this<authentication_statement> {
public:
    authentication_statement() : cql_statement_no_metadata(&timeout_config::other_timeout) {}

    uint32_t get_bound_terms() override;

    std::unique_ptr<prepared> prepare(database& db, cql_stats& stats) override;

    bool uses_function(const sstring& ks_name, const sstring& function_name) const override;

    bool depends_on_keyspace(const sstring& ks_name) const override;

    bool depends_on_column_family(const sstring& cf_name) const override;

    future<> check_access(const service::client_state& state) override;

    void validate(service::storage_proxy&, const service::client_state& state) override;
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), sstring(), sstring());
    }
};

}

}
