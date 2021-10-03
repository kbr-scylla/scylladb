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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "cql3/constants.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

thread_local const ::shared_ptr<constants::value> constants::UNSET_VALUE = ::make_shared<constants::value>(cql3::raw_value::make_unset_value(), empty_type);
thread_local const ::shared_ptr<terminal> constants::NULL_VALUE = ::make_shared<constants::null_value>();

void constants::deleter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        collection_mutation_description coll_m;
        coll_m.tomb = params.make_tombstone();

        m.set_cell(prefix, column, coll_m.serialize(*column.type));
    } else {
        m.set_cell(prefix, column, params.make_dead_cell());
    }
}

expr::expression constants::marker::to_expression() {
    return expr::bind_variable {
        .shape = expr::bind_variable::shape_type::scalar,
        .bind_index = _bind_index,
        .value_type = _receiver->type
    };
}

}
