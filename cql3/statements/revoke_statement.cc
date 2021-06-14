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

#include "revoke_statement.hh"
#include "auth/authorizer.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/query_state.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::revoke_statement::prepare(
                database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<revoke_statement>(*this));
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::revoke_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    auto& auth_service = *state.get_client_state().get_auth_service();

    return auth::revoke_permissions(auth_service, _role_name, _permissions, _resource).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
    }).handle_exception_type([](const auth::nonexistant_role& e) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                exceptions::invalid_request_exception(e.what()));
    }).handle_exception_type([](const auth::unsupported_authorization_operation& e) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                exceptions::invalid_request_exception(e.what()));
    });
}
