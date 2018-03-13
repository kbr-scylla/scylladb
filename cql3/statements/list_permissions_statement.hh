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

#include <experimental/optional>

#include "authorization_statement.hh"
#include "auth/permission.hh"
#include "auth/data_resource.hh"

namespace cql3 {

namespace statements {

class list_permissions_statement : public authorization_statement {
private:
    auth::permission_set _permissions;
    std::experimental::optional<auth::data_resource> _resource;
    std::experimental::optional<sstring> _username;
    bool _recursive;

public:
    list_permissions_statement(auth::permission_set, std::experimental::optional<auth::data_resource>, std::experimental::optional<sstring>, bool);

    void validate(distributed<service::storage_proxy>&, const service::client_state&) override;
    future<> check_access(const service::client_state&) override;
    future<::shared_ptr<cql_transport::messages::result_message>> execute(distributed<service::storage_proxy>&
                    , service::query_state&
                    , const query_options&) override;
};

}

}
