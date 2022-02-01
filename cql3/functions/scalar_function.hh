/*
 */

/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "bytes.hh"
#include "function.hh"
#include <vector>

namespace cql3 {

namespace functions {

class scalar_function : public virtual function {
public:
    /**
     * Applies this function to the specified parameter.
     *
     * @param protocolVersion protocol version used for parameters and return value
     * @param parameters the input parameters
     * @return the result of applying this function to the parameter
     * @throws InvalidRequestException if this function cannot not be applied to the parameter
     */
    virtual bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) = 0;
};


}
}
