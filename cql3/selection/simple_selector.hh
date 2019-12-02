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

#include "cql3/selection/selection.hh"
#include "cql3/selection/selector.hh"

namespace cql3 {

namespace selection {

class simple_selector_factory : public selector::factory {
private:
    const sstring _column_name;
    const uint32_t _idx;
    data_type _type;
public:
    simple_selector_factory(const sstring& column_name, uint32_t idx, data_type type)
        : _column_name(std::move(column_name))
        , _idx(idx)
        , _type(type)
    { }

    virtual sstring column_name() const override {
        return _column_name;
    }

    virtual data_type get_return_type() const override {
        return _type;
    }

    virtual ::shared_ptr<selector> new_instance() const override;
};

class simple_selector : public selector {
private:
    const sstring _column_name;
    const uint32_t _idx;
    data_type _type;
    bytes_opt _current;
    bool _first; ///< Whether the next row we receive is the first in its group.
public:
    static ::shared_ptr<factory> new_factory(const sstring& column_name, uint32_t idx, data_type type) {
        return ::make_shared<simple_selector_factory>(column_name, idx, type);
    }

    simple_selector(const sstring& column_name, uint32_t idx, data_type type)
        : _column_name(std::move(column_name))
        , _idx(idx)
        , _type(type)
        , _first(true)
    { }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        // GROUP BY calls add_input() repeatedly without reset() in between, and it expects the
        // output to be the first value encountered:
        // https://cassandra.apache.org/doc/latest/cql/dml.html#grouping-results
        if (_first) {
            // TODO: can we steal it?
            _current = (*rs.current)[_idx];
            _first = false;
        }
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        return std::move(_current);
    }

    virtual void reset() override {
        _current = {};
        _first = true;
    }

    virtual data_type get_type() const override {
        return _type;
    }

    virtual sstring assignment_testable_source_context() const override {
        return _column_name;
    }

#if 0
    @Override
    public String toString()
    {
        return columnName;
    }
#endif
};

}

}
