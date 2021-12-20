/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/create_function_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_function.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "lang/lua.hh"
#include "data_dictionary/data_dictionary.hh"
#include "database.hh" // for wasm
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

shared_ptr<functions::function> create_function_statement::create(service::storage_proxy& proxy, functions::function* old) const {
    if (old && !dynamic_cast<functions::user_function*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined function", *old));
    }
    if (_language != "lua" && _language != "xwasm") {
        throw exceptions::invalid_request_exception(format("Language '{}' is not supported", _language));
    }
    data_type return_type = prepare_type(proxy, *_return_type);
    std::vector<sstring> arg_names;
    for (const auto& arg_name : _arg_names) {
        arg_names.push_back(arg_name->to_string());
    }

    auto&& db = proxy.data_dictionary();
    if (_language == "lua") {
        auto cfg = lua::make_runtime_config(db.get_config());
        functions::user_function::context ctx = functions::user_function::lua_context {
            .bitcode = lua::compile(cfg, arg_names, _body),
            .cfg = cfg,
        };

        return ::make_shared<functions::user_function>(_name, _arg_types, std::move(arg_names), _body, _language,
            std::move(return_type), _called_on_null_input, std::move(ctx));
    } else if (_language == "xwasm") {
       // FIXME: need better way to test wasm compilation without real_database()
       wasm::context ctx{db.real_database().wasm_engine(), _name.name};
       try {
            wasm::compile(ctx, arg_names, _body);
            return ::make_shared<functions::user_function>(_name, _arg_types, std::move(arg_names), _body, _language,
                std::move(return_type), _called_on_null_input, std::move(ctx));
       } catch (const wasm::exception& we) {
           throw exceptions::invalid_request_exception(we.what());
       }
    }
    return nullptr;
}

audit::statement_category
create_function_statement::category() const {
    return audit::statement_category::DDL;
}

audit::audit_info_ptr
create_function_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), sstring(), sstring());
}

std::unique_ptr<prepared_statement> create_function_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(audit_info(), make_shared<create_function_statement>(*this));
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
create_function_statement::prepare_schema_mutations(query_processor& qp) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    auto func = dynamic_pointer_cast<functions::user_function>(validate_while_executing(qp.proxy()));

    if (func) {
        m = co_await qp.get_migration_manager().prepare_new_function_announcement(func);
        ret = create_schema_change(*func, true);
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

create_function_statement::create_function_statement(functions::function_name name, sstring language, sstring body,
        std::vector<shared_ptr<column_identifier>> arg_names, std::vector<shared_ptr<cql3_type::raw>> arg_types,
        shared_ptr<cql3_type::raw> return_type, bool called_on_null_input, bool or_replace, bool if_not_exists)
    : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists),
      _language(std::move(language)), _body(std::move(body)), _arg_names(std::move(arg_names)),
      _return_type(std::move(return_type)), _called_on_null_input(called_on_null_input) {}
}
}
