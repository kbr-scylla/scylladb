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
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "types.hh"
#include "native_scalar_function.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"

namespace cql3 {
namespace functions {

class token_fct: public native_scalar_function {
private:
    schema_ptr _schema;

public:
    token_fct(schema_ptr s)
            : native_scalar_function("token",
                    dht::token::get_token_validator(),
                    s->partition_key_type()->types())
                    , _schema(s) {
    }

    bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override {
        auto key = partition_key::from_optional_exploded(*_schema, parameters);
        auto tok = dht::global_partitioner().get_token(*_schema, key);
        warn(unimplemented::cause::VALIDATION);
        return tok.data();
    }
};

}
}
