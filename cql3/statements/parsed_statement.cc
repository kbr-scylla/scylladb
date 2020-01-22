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
 * Copyright (C) 2014 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "raw/parsed_statement.hh"

#include "prepared_statement.hh"

namespace cql3 {

namespace statements {

namespace raw {

parsed_statement::~parsed_statement()
{ }

variable_specifications& parsed_statement::get_bound_variables() {
    return _variables;
}

const variable_specifications& parsed_statement::get_bound_variables() const {
    return _variables;
}

// Used by the parser and preparable statement
void parsed_statement::set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names) {
    _variables.set_bound_variables(bound_names);
}

bool parsed_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return false;
}

}

prepared_statement::prepared_statement(audit::audit_info_ptr&& audit_info, ::shared_ptr<cql_statement> statement_, std::vector<::shared_ptr<column_specification>> bound_names_, std::vector<uint16_t> partition_key_bind_indices)
    : statement(std::move(statement_))
    , bound_names(std::move(bound_names_))
    , partition_key_bind_indices(std::move(partition_key_bind_indices))
{
    statement->set_audit_info(std::move(audit_info));
}

prepared_statement::prepared_statement(audit::audit_info_ptr&& audit_info, ::shared_ptr<cql_statement> statement_, const variable_specifications& names, const std::vector<uint16_t>& partition_key_bind_indices)
    : prepared_statement(std::move(audit_info), statement_, names.get_specifications(), partition_key_bind_indices)
{ }

prepared_statement::prepared_statement(audit::audit_info_ptr&& audit_info, ::shared_ptr<cql_statement> statement_, variable_specifications&& names, std::vector<uint16_t>&& partition_key_bind_indices)
    : prepared_statement(std::move(audit_info), statement_, std::move(names).get_specifications(), std::move(partition_key_bind_indices))
{ }

prepared_statement::prepared_statement(audit::audit_info_ptr&& audit_info, ::shared_ptr<cql_statement>&& statement_)
    : prepared_statement(std::move(audit_info), statement_, std::vector<::shared_ptr<column_specification>>(), std::vector<uint16_t>())
{ }

}

}
