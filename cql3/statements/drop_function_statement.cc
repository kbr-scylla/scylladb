/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_function_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_function.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "cql3/query_processor.hh"
#include "mutation.hh"

namespace cql3 {

namespace statements {

audit::statement_category
drop_function_statement::category() const {
    return audit::statement_category::DDL;
}

audit::audit_info_ptr
drop_function_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

std::unique_ptr<prepared_statement> drop_function_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<drop_function_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_function_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = validate_while_executing(qp);

    if (func) {
        auto user_func = dynamic_pointer_cast<functions::user_function>(func);
        if (!user_func) {
            throw exceptions::invalid_request_exception(format("'{}' is not a user defined function", func));
        }
        if (auto aggregate = functions::functions::used_by_user_aggregate(user_func->name()); bool(aggregate)) {
            throw exceptions::invalid_request_exception(format("Cannot delete function {}, as it is used by user-defined aggregate {}", func, *aggregate));
        }
        m = co_await qp.get_migration_manager().prepare_function_drop_announcement(user_func);
        ret = create_schema_change(*func, false);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

drop_function_statement::drop_function_statement(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : drop_function_statement_base(std::move(name), std::move(arg_types), args_present, if_exists) {}

}
}
