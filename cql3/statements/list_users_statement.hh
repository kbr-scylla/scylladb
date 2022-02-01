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

#include "authentication_statement.hh"

namespace cql3 {

class query_processor;

namespace statements {

class list_users_statement : public authentication_statement {
public:

    std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

    void validate(query_processor&, const service::client_state&) const override;
    future<> check_access(query_processor& qp, const service::client_state&) const override;
    future<::shared_ptr<cql_transport::messages::result_message>> execute(query_processor&
                    , service::query_state&
                    , const query_options&) const override;
};

}

}
