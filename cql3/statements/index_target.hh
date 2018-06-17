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

#pragma once

#include "core/shared_ptr.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace statements {

struct index_target {
    static const sstring target_option_name;
    static const sstring custom_index_option_name;

    enum class target_type {
        values, keys, keys_and_values, full
    };

    const ::shared_ptr<column_identifier> column;
    const target_type type;

    index_target(::shared_ptr<column_identifier> c, target_type t)
            : column(c), type(t) {
    }

    sstring as_cql_string(schema_ptr schema) const;
    sstring as_string() const;

    static sstring index_option(target_type type);
    static target_type from_column_definition(const column_definition& cd);
    static index_target::target_type from_sstring(const sstring& s);

    class raw {
    public:
        const ::shared_ptr<column_identifier::raw> column;
        const target_type type;

        raw(::shared_ptr<column_identifier::raw> c, target_type t)
                : column(c), type(t)
        {}

        static ::shared_ptr<raw> values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_and_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> full_collection(::shared_ptr<column_identifier::raw> c);
        ::shared_ptr<index_target> prepare(schema_ptr);
    };
};

sstring to_sstring(index_target::target_type type);

}
}
