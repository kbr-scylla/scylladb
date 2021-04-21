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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/restrictions/restriction.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/update_parameters.hh"
#include "cql3/column_condition.hh"
#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "cql3/relation.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future-util.hh>

#include "unimplemented.hh"
#include "validation.hh"

#include <memory>

namespace cql3 {

namespace statements {

class modification_statement;

namespace raw {

class modification_statement : public cf_statement {
    sstring _raw_cql;
public:
    using conditions_vector = std::vector<std::pair<::shared_ptr<column_identifier::raw>, lw_shared_ptr<column_condition::raw>>>;
protected:
    const std::unique_ptr<attributes::raw> _attrs;
    const std::vector<std::pair<::shared_ptr<column_identifier::raw>, lw_shared_ptr<column_condition::raw>>> _conditions;
private:
    const bool _if_not_exists;
    const bool _if_exists;
protected:
    modification_statement(cf_name name, std::unique_ptr<attributes::raw> attrs, conditions_vector conditions, bool if_not_exists, bool if_exists);

public:
    virtual std::unique_ptr<prepared_statement> prepare(database& db, cql_stats& stats) override;
    ::shared_ptr<cql3::statements::modification_statement> prepare(database& db, variable_specifications& bound_names, cql_stats& stats) const;
    void add_raw(sstring&& raw) { _raw_cql = std::move(raw); }
    const sstring& get_raw_cql() const { return _raw_cql; }
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(database& db, schema_ptr schema,
        variable_specifications& bound_names, std::unique_ptr<attributes> attrs, cql_stats& stats) const = 0;

    // Helper function used by child classes to prepare conditions for a prepared statement.
    // Must be called before processing WHERE clause, because to perform sanity checks there
    // we need to know what kinds of conditions (static, regular) the statement has.
    void prepare_conditions(database& db, const schema& schema, variable_specifications& bound_names,
            cql3::statements::modification_statement& stmt) const;

    virtual audit::statement_category category() const override;
};

}

}

}
