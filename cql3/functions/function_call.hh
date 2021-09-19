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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "function.hh"
#include "scalar_function.hh"
#include "cql3/term.hh"
#include "exceptions/exceptions.hh"
#include "cql3/functions/function_name.hh"

namespace cql3::expr {

struct function_call;

}

namespace cql3 {
namespace functions {

class function_call : public non_terminal {
    const shared_ptr<scalar_function> _fun;
    const std::vector<shared_ptr<term>> _terms;
    // 0-based index of the function call within a CQL statement.
    // Used to populate the cache of execution results while passing to
    // another shard (handling `bounce_to_shard` messages) in LWT statements.
    //
    // The id is set only for the function calls that are a part of LWT
    // statement restrictions for the partition key. Otherwise, the id is not
    // set and the call is not considered when using or populating the cache.
    std::optional<uint8_t> _id;
public:
    function_call(shared_ptr<scalar_function> fun, std::vector<shared_ptr<term>> terms)
            : _fun(std::move(fun)), _terms(std::move(terms)) {
    }
    virtual void fill_prepare_context(prepare_context& ctx) const override;
    void set_id(std::optional<uint8_t> id) {
        _id = id;
    }
    virtual shared_ptr<terminal> bind(const query_options& options) override;
public:
    virtual bool contains_bind_marker() const override;
private:
    cql3::raw_value_view bind_and_get_internal(const query_options& options);
};

::shared_ptr<term> prepare_function_call(const expr::function_call& fc, database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver);

assignment_testable::test_result test_assignment_function_call(const cql3::expr::function_call& fc, database& db, const sstring& keyspace, const column_specification& receiver);

}
}
