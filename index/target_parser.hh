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
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/index_target.hh"

namespace secondary_index {

struct target_parser {
    struct target_info {
        std::vector<const column_definition*> pk_columns;
        std::vector<const column_definition*> ck_columns;
        cql3::statements::index_target::target_type type;
    };

    static target_info parse(schema_ptr schema, const index_metadata& im);

    static target_info parse(schema_ptr schema, const sstring& target);

    static bool is_local(sstring target_string);

    static sstring get_target_column_name_from_string(const sstring& targets);

    static sstring serialize_targets(const std::vector<::shared_ptr<cql3::statements::index_target>>& targets);
};

}
