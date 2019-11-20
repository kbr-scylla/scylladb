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
 * Copyright (C) 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <ostream>

namespace cql3 {

namespace statements {

class statement_type final {
    enum class type : size_t {
        insert = 0,
        update,
        del,
        select,

        last  // Keep me as last entry
    };
    const type _type;

    statement_type(type t) : _type(t) {
    }
public:
    statement_type() = delete;

    bool is_insert() const {
        return _type == type::insert;
    }
    bool is_update() const {
        return _type == type::update;
    }
    bool is_delete() const {
        return _type == type::del;
    }
    bool is_select() const {
        return _type == type::select;
    }

    static const statement_type INSERT;
    static const statement_type UPDATE;
    static const statement_type DELETE;
    static const statement_type SELECT;

    static constexpr size_t MAX_VALUE = size_t(type::last) - 1;

    explicit operator size_t() const {
        return size_t(_type);
    }

    bool operator==(const statement_type& other) const {
        return _type == other._type;
    }

    bool operator!=(const statement_type& other) const {
        return !(_type == other._type);
    }

    friend std::ostream &operator<<(std::ostream &os, const statement_type& t) {
        switch (t._type) {
        case type::insert: return os << "INSERT";
        case type::update: return os << "UPDATE";
        case type::del: return os << "DELETE";
        case type::select : return os << "SELECT";

        case type::last : return os << "LAST";
        }
        return os;
    }
};

}
}
