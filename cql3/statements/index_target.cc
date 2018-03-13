/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <stdexcept>
#include "index_target.hh"
#include "db/index/secondary_index.hh"

namespace cql3 {

namespace statements {

using db::index::secondary_index;

const sstring index_target::target_option_name = "target";
const sstring index_target::custom_index_option_name = "class_name";

sstring index_target::as_cql_string(schema_ptr schema) const {
    if (!schema->get_column_definition(column->name())->type->is_collection()) {
        return column->to_cql_string();
    }
    return sprint("%s(%s)", to_sstring(type), column->to_cql_string());
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
    throw std::runtime_error(sprint("Unknown target type: %s", s));
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

::shared_ptr<index_target>
index_target::raw::prepare(schema_ptr schema) {
    return ::make_shared<index_target>(column->prepare_column_identifier(schema), type);
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
