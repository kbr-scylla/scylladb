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

#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "database_fwd.hh"

namespace cql3 {

class relation;

namespace statements {

class modification_statement;

namespace raw {

class delete_statement : public modification_statement {
private:
    std::vector<std::unique_ptr<operation::raw_deletion>> _deletions;
    std::vector<::shared_ptr<relation>> _where_clause;
public:
    delete_statement(cf_name name,
           std::unique_ptr<attributes::raw> attrs,
           std::vector<std::unique_ptr<operation::raw_deletion>> deletions,
           std::vector<::shared_ptr<relation>> where_clause,
           conditions_vector conditions,
           bool if_exists);
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(database& db, schema_ptr schema,
        prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;
};

}

}

}
