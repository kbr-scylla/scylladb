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
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>

#include "types.hh"

#include "seastarx.hh"

class user_types_metadata;
class types_metadata;
class keyspace_metadata;

namespace db {
namespace cql_type_parser {

data_type parse(const sstring& keyspace, const sstring& type);

class raw_builder {
public:
    raw_builder(keyspace_metadata &ks);
    ~raw_builder();

    void add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types);
    std::vector<user_type> build();
private:
    class impl;
    std::unique_ptr<impl>
        _impl;
};

}
}
