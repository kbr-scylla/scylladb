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
 * Copyright (C) 2017-present ScyllaDB
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

#include <seastar/core/shared_ptr.hh>
#include <optional>
#include <memory>

#include "schema_fwd.hh"

namespace cql3 {

class query_processor;
class index_name;

namespace statements {

class drop_index_statement : public schema_altering_statement {
    sstring _index_name;

    // A "drop index" statement does not specify the base table's name, just an
    // index name. Nevertheless, the virtual column_family() method is supposed
    // to return a reasonable table name. If the index doesn't exist, we return
    // an empty name (this commonly happens with "if exists").
    mutable std::optional<sstring> _cf_name;
    bool _if_exists;
    cql_stats* _cql_stats = nullptr;
public:
    drop_index_statement(::shared_ptr<index_name> index_name, bool if_exists);

    virtual const sstring& column_family() const override;

    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;

    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_processor& qp) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
private:
    schema_ptr lookup_indexed_table(service::storage_proxy& proxy) const;
    schema_ptr make_drop_idex_schema(query_processor& qp) const;
};

}

}
