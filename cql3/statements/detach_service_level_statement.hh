/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "cql3/statements/service_level_statement.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {
namespace statements {

class detach_service_level_statement final : public service_level_statement {
    sstring _role_name;
public:
    detach_service_level_statement(sstring role_name);
    std::unique_ptr<cql3::statements::prepared_statement> prepare(data_dictionary::database db, cql_stats &stats) override;
    void validate(query_processor&, const service::client_state&) const override;
    virtual future<> check_access(query_processor& qp, const service::client_state&) const override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor&, service::query_state&, const query_options&) const override;
};

}

}
