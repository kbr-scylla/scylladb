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

#include "cql3/abstract_marker.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/user_types.hh"
#include "cql3/prepare_context.hh"
#include "types/list.hh"

namespace cql3 {

abstract_marker::abstract_marker(int32_t bind_index, lw_shared_ptr<column_specification>&& receiver)
    : _bind_index{bind_index}
    , _receiver{std::move(receiver)}
{ }

void abstract_marker::fill_prepare_context(prepare_context& ctx) const {
    ctx.add_variable_specification(_bind_index, _receiver);
}

bool abstract_marker::contains_bind_marker() const {
    return true;
}

}
