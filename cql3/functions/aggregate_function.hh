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
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "function.hh"
#include <optional>

namespace cql3 {
namespace functions {


/**
 * Performs a calculation on a set of values and return a single value.
 */
class aggregate_function : public virtual function {
public:
    class aggregate;

    /**
     * Creates a new <code>Aggregate</code> instance.
     *
     * @return a new <code>Aggregate</code> instance.
     */
    virtual std::unique_ptr<aggregate> new_aggregate() = 0;

    /**
     * An aggregation operation.
     */
    class aggregate {
    public:
        using opt_bytes = aggregate_function::opt_bytes;

        virtual ~aggregate() {}

        /**
         * Adds the specified input to this aggregate.
         *
         * @param protocol_version native protocol version
         * @param values the values to add to the aggregate.
         */
        virtual void add_input(cql_serialization_format sf, const std::vector<opt_bytes>& values) = 0;

        /**
         * Computes and returns the aggregate current value.
         *
         * @param protocol_version native protocol version
         * @return the aggregate current value.
         */
        virtual opt_bytes compute(cql_serialization_format sf) = 0;

        /**
         * Reset this aggregate.
         */
        virtual void reset() = 0;
    };
};

}
}
