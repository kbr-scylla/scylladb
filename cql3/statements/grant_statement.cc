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

#include "grant_statement.hh"
#include "auth/authorizer.hh"

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::grant_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto& client_state = state.get_client_state();
    auto& auth_service = *client_state.get_auth_service();

    return auth_service.underlying_authorizer().grant(client_state.user(), _permissions, _resource, _username).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
    });
}
