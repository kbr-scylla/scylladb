/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "cql3/statements/use_statement.hh"
#include "cql3/statements/raw/use_statement.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

use_statement::use_statement(sstring keyspace)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _keyspace(keyspace)
{
}

uint32_t use_statement::get_bound_terms() const
{
    return 0;
}

namespace raw {

use_statement::use_statement(sstring keyspace)
    : _keyspace(keyspace)
{
}

std::unique_ptr<prepared_statement> use_statement::prepare(data_dictionary::database db, cql_stats& stats)
{
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<cql3::statements::use_statement>(_keyspace));
}

audit::statement_category use_statement::category() const {
    // It's not obvious why USE is a DML but that's how Origin classifies it.
    return audit::statement_category::DML;
}

}

bool use_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool use_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> use_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    state.validate_login();
    return make_ready_future<>();
}

void use_statement::validate(query_processor&, const service::client_state& state) const
{
}

future<::shared_ptr<cql_transport::messages::result_message>>
use_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    state.get_client_state().set_keyspace(qp.db().real_database(), _keyspace);
    auto result =::make_shared<cql_transport::messages::result_message::set_keyspace>(_keyspace);
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
}

}

}
