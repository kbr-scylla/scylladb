/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include <stdexcept>
#include "index_target.hh"
#include "index/secondary_index.hh"
#include <boost/algorithm/string/join.hpp>

namespace cql3 {

namespace statements {

using db::index::secondary_index;

const sstring index_target::target_option_name = "target";
const sstring index_target::custom_index_option_name = "class_name";

sstring index_target::as_string() const {
    struct as_string_visitor {
        sstring operator()(const std::vector<::shared_ptr<column_identifier>>& columns) const {
            return "(" + boost::algorithm::join(columns | boost::adaptors::transformed(
                    [](const ::shared_ptr<cql3::column_identifier>& ident) -> sstring {
                        return ident->to_string();
                    }), ",") + ")";
        }

        sstring operator()(const ::shared_ptr<column_identifier>& column) const {
            return column->to_string();
        }
    };

    return std::visit(as_string_visitor(), value);
}

index_target::target_type index_target::from_sstring(const sstring& s)
{
    if (s == "keys") {
        return index_target::target_type::keys;
    } else if (s == "entries") {
        return index_target::target_type::keys_and_values;
    } else if (s == "values") {
        return index_target::target_type::values;
    } else if (s == "full") {
        return index_target::target_type::full;
    }
    throw std::runtime_error(format("Unknown target type: {}", s));
}

sstring index_target::index_option(target_type type) {
    switch (type) {
        case target_type::keys: return secondary_index::index_keys_option_name;
        case target_type::keys_and_values: return secondary_index::index_entries_option_name;
        case target_type::values: return secondary_index::index_values_option_name;
        default: throw std::invalid_argument("should not reach");
    }
}

::shared_ptr<index_target::raw>
index_target::raw::values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::values);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_and_values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys_and_values);
}

::shared_ptr<index_target::raw>
index_target::raw::full_collection(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::full);
}

::shared_ptr<index_target::raw>
index_target::raw::columns(std::vector<::shared_ptr<column_identifier::raw>> c) {
    return ::make_shared<raw>(std::move(c), target_type::values);
}

::shared_ptr<index_target>
index_target::raw::prepare(const schema& s) const {
    struct prepare_visitor {
        const schema& _schema;
        target_type _type;

        ::shared_ptr<index_target> operator()(const std::vector<::shared_ptr<column_identifier::raw>>& columns) const {
            auto prepared_idents = boost::copy_range<std::vector<::shared_ptr<column_identifier>>>(
                    columns | boost::adaptors::transformed([this] (const ::shared_ptr<column_identifier::raw>& raw_ident) {
                        return raw_ident->prepare_column_identifier(_schema);
                    })
            );
            return ::make_shared<index_target>(std::move(prepared_idents), _type);
        }

        ::shared_ptr<index_target> operator()(::shared_ptr<column_identifier::raw> raw_ident) const {
            return ::make_shared<index_target>(raw_ident->prepare_column_identifier(_schema), _type);
        }
    };

    return std::visit(prepare_visitor{s, type}, value);
}

sstring to_sstring(index_target::target_type type)
{
    switch (type) {
    case index_target::target_type::keys: return "keys";
    case index_target::target_type::keys_and_values: return "entries";
    case index_target::target_type::values: return "values";
    case index_target::target_type::full: return "full";
    }
    return "";
}

}

}
