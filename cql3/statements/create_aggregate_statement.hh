/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "cql3/statements/function_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/expr/expression.hh"
#include <optional>

namespace cql3 {

class query_processor;

namespace functions {
    class user_aggregate;
}

namespace statements {

class create_aggregate_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>> prepare_schema_mutations(query_processor& qp, api::timestamp_type) const override;

    virtual shared_ptr<functions::function> create(query_processor& qp, functions::function* old) const override;

    sstring _sfunc;
    shared_ptr<cql3_type::raw> _stype;
    std::optional<sstring> _ffunc;
    std::optional<expr::expression> _ival;

protected:
    virtual audit::audit_info_ptr audit_info() const override;
    virtual audit::statement_category category() const override;
public:
    create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, std::optional<sstring> ffunc, std::optional<expr::expression> ival, bool or_replace, bool if_not_exists);
};
}
}
