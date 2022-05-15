/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"
#include <variant>

namespace cql3 {

namespace statements {

struct index_target {
    static const sstring target_option_name;
    static const sstring custom_index_option_name;

    using single_column =::shared_ptr<column_identifier>;
    using multiple_columns = std::vector<::shared_ptr<column_identifier>>;
    using value_type = std::variant<single_column, multiple_columns>;

    enum class target_type {
        values, keys, keys_and_values, full
    };

    const value_type value;
    const target_type type;

    index_target(::shared_ptr<column_identifier> c, target_type t) : value(c) , type(t) {}
    index_target(std::vector<::shared_ptr<column_identifier>> c, target_type t) : value(std::move(c)), type(t) {}

    sstring as_string() const;

    static sstring index_option(target_type type);
    static target_type from_column_definition(const column_definition& cd);
    static index_target::target_type from_sstring(const sstring& s);

    class raw {
    public:
        using single_column = ::shared_ptr<column_identifier::raw>;
        using multiple_columns = std::vector<::shared_ptr<column_identifier::raw>>;
        using value_type = std::variant<single_column, multiple_columns>;

        const value_type value;
        const target_type type;

        raw(::shared_ptr<column_identifier::raw> c, target_type t) : value(c), type(t) {}
        raw(std::vector<::shared_ptr<column_identifier::raw>> pk_columns, target_type t) : value(pk_columns), type(t) {}

        static ::shared_ptr<raw> values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_and_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> full_collection(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> columns(std::vector<::shared_ptr<column_identifier::raw>> c);
        ::shared_ptr<index_target> prepare(const schema&) const;
    };
};

sstring to_sstring(index_target::target_type type);

}
}
