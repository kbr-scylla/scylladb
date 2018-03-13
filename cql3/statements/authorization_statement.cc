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

#include "authorization_statement.hh"
#include "transport/messages/result_message.hh"

uint32_t cql3::statements::authorization_statement::get_bound_terms() {
    return 0;
}

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::authorization_statement::prepare(
                database& db, cql_stats& stats) {
    return std::make_unique<parsed_statement::prepared>(audit_info(), this->shared_from_this());
}

bool cql3::statements::authorization_statement::uses_function(
                const sstring& ks_name, const sstring& function_name) const {
    return parsed_statement::uses_function(ks_name, function_name);
}

bool cql3::statements::authorization_statement::depends_on_keyspace(
                const sstring& ks_name) const {
    return false;
}

bool cql3::statements::authorization_statement::depends_on_column_family(
                const sstring& cf_name) const {
    return false;
}

void cql3::statements::authorization_statement::validate(
                distributed<service::storage_proxy>&,
                const service::client_state& state) {
}

future<> cql3::statements::authorization_statement::check_access(const service::client_state& state) {
    return make_ready_future<>();
}

future<::shared_ptr<cql_transport::messages::result_message>> cql3::statements::authorization_statement::execute_internal(
                distributed<service::storage_proxy>& proxy,
                service::query_state& state, const query_options& options) {
    // Internal queries are exclusively on the system keyspace and makes no sense here
    throw std::runtime_error("unsupported operation");
}

void cql3::statements::authorization_statement::mayme_correct_resource(auth::data_resource& resource, const service::client_state& state) {
    if (resource.is_column_family_level() && resource.keyspace().empty()) {
        resource = auth::data_resource(state.get_keyspace(), resource.column_family());
    }
}

audit::statement_category cql3::statements::authorization_statement::category() const {
    return audit::statement_category::DCL;
}

