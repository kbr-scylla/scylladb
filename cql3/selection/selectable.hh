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

#include "schema_fwd.hh"
#include <seastar/core/shared_ptr.hh>
#include "cql3/selection/selector.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/expr/expression.hh"

namespace cql3 {

namespace selection {

class selectable;

class selectable {
public:
    virtual ~selectable() {}
    virtual ::shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr schema, std::vector<const column_definition*>& defs) = 0;
    virtual sstring to_string() const = 0;
protected:
    static size_t add_and_get_index(const column_definition& def, std::vector<const column_definition*>& defs) {
        auto i = std::find(defs.begin(), defs.end(), &def);
        if (i != defs.end()) {
            return std::distance(defs.begin(), i);
        }
        defs.push_back(&def);
        return defs.size() - 1;
    }
public:
    class writetime_or_ttl;

    class with_function;
    class with_anonymous_function;

    class with_field_selection;

    class with_cast;
};

std::ostream & operator<<(std::ostream &os, const selectable& s);

class selectable::with_function : public selectable {
    functions::function_name _function_name;
    std::vector<shared_ptr<selectable>> _args;
public:
    with_function(functions::function_name fname, std::vector<shared_ptr<selectable>> args)
        : _function_name(std::move(fname)), _args(std::move(args)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

expr::expression make_count_rows_function_expression();

class selectable::with_anonymous_function : public selectable {
    shared_ptr<functions::function> _function;
    std::vector<shared_ptr<selectable>> _args;
public:
    with_anonymous_function(::shared_ptr<functions::function> f, std::vector<shared_ptr<selectable>> args)
        : _function(f), _args(std::move(args)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

class selectable::with_cast : public selectable {
    ::shared_ptr<selectable> _arg;
    cql3_type _type;
public:
    with_cast(::shared_ptr<selectable> arg, cql3_type type)
        : _arg(std::move(arg)), _type(std::move(type)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

shared_ptr<selectable> prepare_selectable(const schema& s, const expr::expression& raw_selectable);
bool selectable_processes_selection(const expr::expression& raw_selectable);

}

}
