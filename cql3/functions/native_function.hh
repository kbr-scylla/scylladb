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

#include "abstract_function.hh"

namespace cql3 {
namespace functions {

/**
 * Base class for our native/hardcoded functions.
 */
class native_function : public abstract_function {
protected:
    native_function(sstring name, data_type return_type, std::vector<data_type> arg_types)
        : abstract_function(function_name::native_function(std::move(name)),
                std::move(arg_types), std::move(return_type)) {
    }

public:
    // Most of our functions are pure, the other ones should override this
    virtual bool is_pure() const override {
        return true;
    }

    virtual bool is_native() const override {
        return true;
    }
};

}
}
