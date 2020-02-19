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

#include "selectable.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class selectable::with_field_selection : public selectable {
public:
    shared_ptr<selectable> _selected;
    shared_ptr<column_identifier> _field;
public:
    with_field_selection(shared_ptr<selectable> selected, shared_ptr<column_identifier> field)
            : _selected(std::move(selected)), _field(std::move(field)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) override;

    class raw : public selectable::raw {
        shared_ptr<selectable::raw> _selected;
        shared_ptr<column_identifier::raw> _field;
    public:
        raw(shared_ptr<selectable::raw> selected, shared_ptr<column_identifier::raw> field)
                : _selected(std::move(selected)), _field(std::move(field)) {
        }
        virtual shared_ptr<selectable> prepare(const schema& s) override;
        virtual bool processes_selection() const override;
    };
};

}

}
