/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_aggregate_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "cql3/query_processor.hh"
#include "mutation.hh"

namespace cql3 {

namespace statements {

std::unique_ptr<prepared_statement> drop_aggregate_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<drop_aggregate_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_aggregate_statement::prepare_schema_mutations(query_processor& qp, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = validate_while_executing(qp);
    if (func) {
        auto user_aggr = dynamic_pointer_cast<functions::user_aggregate>(func);
        if (!user_aggr) {
            throw exceptions::invalid_request_exception(format("'{}' is not a user defined aggregate", func));
        }
        m = co_await qp.get_migration_manager().prepare_aggregate_drop_announcement(user_aggr, ts);
        ret = create_schema_change(*func, false);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

drop_aggregate_statement::drop_aggregate_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

audit::statement_category drop_aggregate_statement::category() const {
    return audit::statement_category::DDL;
}

audit::audit_info_ptr
drop_aggregate_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

}
}
