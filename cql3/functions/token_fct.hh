/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "types.hh"
#include "native_scalar_function.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"
#include "unimplemented.hh"

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
        auto tok = dht::get_token(*_schema, key);
        warn(unimplemented::cause::VALIDATION);
        return tok.data();
    }
};

}
}
