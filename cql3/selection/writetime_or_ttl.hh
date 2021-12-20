/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

#pragma once

#include "selectable.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class selectable::writetime_or_ttl : public selectable {
public:
    shared_ptr<column_identifier> _id;
    bool _is_writetime;

    writetime_or_ttl(shared_ptr<column_identifier> id, bool is_writetime)
            : _id(std::move(id)), _is_writetime(is_writetime) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

}

}
