/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "user_function.hh"
#include "log.hh"
#include "cql_serialization_format.hh"

#include <seastar/core/thread.hh>

namespace cql3 {
namespace functions {

extern logging::logger log;

user_function::user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names,
        sstring body, sstring language, data_type return_type, bool called_on_null_input, context ctx)
    : abstract_function(std::move(name), std::move(arg_types), std::move(return_type)),
      _arg_names(std::move(arg_names)), _body(std::move(body)), _language(std::move(language)),
      _called_on_null_input(called_on_null_input), _ctx(std::move(ctx)) {}

bool user_function::is_pure() const { return true; }

bool user_function::is_native() const { return false; }

bool user_function::is_aggregate() const { return false; }

bool user_function::requires_thread() const { return true; }

bytes_opt user_function::execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) {
    const auto& types = arg_types();
    if (parameters.size() != types.size()) {
        throw std::logic_error("Wrong number of parameters");
    }

    if (!seastar::thread::running_in_thread()) {
        on_internal_error(log, "User function cannot be executed in this context");
    }
    return seastar::visit(_ctx,
        [&] (lua_context& ctx) -> bytes_opt {
            std::vector<data_value> values;
            values.reserve(parameters.size());
            for (int i = 0, n = types.size(); i != n; ++i) {
                const data_type& type = types[i];
                const bytes_opt& bytes = parameters[i];
                if (!bytes && !_called_on_null_input) {
                    return std::nullopt;
                }
                values.push_back(bytes ? type->deserialize(*bytes) : data_value::make_null(type));
            }
            return lua::run_script(lua::bitcode_view{ctx.bitcode}, values, return_type(), ctx.cfg).get0();
        },
        [&] (wasm::context& ctx) {
            try {
                return wasm::run_script(ctx, arg_types(), parameters, return_type(), _called_on_null_input).get0();
            } catch (const wasm::exception& e) {
                throw exceptions::invalid_request_exception(format("UDF error: {}", e.what()));
            }
        });
}

}
}
