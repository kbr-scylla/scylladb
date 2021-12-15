/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/create_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "gms/feature_service.hh"

namespace cql3 {

namespace statements {

shared_ptr<functions::function> create_aggregate_statement::create(service::storage_proxy& proxy, functions::function* old) const {
    if (!proxy.features().cluster_supports_user_defined_aggregates()) {
        throw exceptions::invalid_request_exception("Cluster does not support user-defined aggregates, upgrade the whole cluster in order to use UDA");
    }
    if (old && !dynamic_cast<functions::user_aggregate*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined aggregate", *old));
    }
    data_type state_type = prepare_type(proxy, *_stype);

    auto&& db = proxy.get_db().local();
    std::vector<data_type> acc_types{state_type};
    acc_types.insert(acc_types.end(), _arg_types.begin(), _arg_types.end());
    auto state_func = dynamic_pointer_cast<functions::scalar_function>(functions::functions::find(functions::function_name{_name.keyspace, _sfunc}, acc_types));
    auto final_func = dynamic_pointer_cast<functions::scalar_function>(functions::functions::find(functions::function_name{_name.keyspace, _ffunc}, {state_type}));
 
    if (!state_func) {
        throw exceptions::invalid_request_exception(format("State function not found: {}", _sfunc));
    }
    if (!final_func) {
        throw exceptions::invalid_request_exception(format("Final function not found: {}", _ffunc));
    }

    auto dummy_ident = ::make_shared<column_identifier>("", true);
    auto column_spec = make_lw_shared<column_specification>("", "", dummy_ident, state_type);
    auto initcond_term = expr::evaluate(prepare_expression(_ival, db, _name.keyspace, {column_spec}), query_options::DEFAULT);
    bytes_opt initcond = std::move(initcond_term.value).to_bytes();

    return ::make_shared<functions::user_aggregate>(_name, initcond, std::move(state_func), std::move(final_func));
}

std::unique_ptr<prepared_statement> create_aggregate_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<create_aggregate_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
create_aggregate_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto aggregate = dynamic_pointer_cast<functions::user_aggregate>(validate_while_executing(qp.proxy()));
    if (aggregate) {
        m = co_await qp.get_migration_manager().prepare_new_aggregate_announcement(aggregate);
        ret = create_schema_change(*aggregate, true);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

create_aggregate_statement::create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, sstring ffunc, expr::expression ival, bool or_replace, bool if_not_exists)
        : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists)
        , _sfunc(std::move(sfunc))
        , _stype(std::move(stype))
        , _ffunc(std::move(ffunc))
        , _ival(std::move(ival))
    {}

audit::statement_category create_aggregate_statement::category() const {
    return audit::statement_category::DDL;
}

audit::audit_info_ptr
create_aggregate_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

}

}
