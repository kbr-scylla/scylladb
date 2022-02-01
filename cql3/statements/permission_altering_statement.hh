/*
 */

/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "authorization_statement.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"

namespace cql3 {

class role_name;

namespace statements {

class permission_altering_statement : public authorization_statement {
protected:
    auth::permission_set _permissions;
    mutable auth::resource _resource;
    sstring _role_name;

public:
    permission_altering_statement(auth::permission_set, auth::resource, const cql3::role_name&);

    void validate(query_processor&, const service::client_state&) const override;
    future<> check_access(query_processor& qp, const service::client_state&) const override;
};

}

}
