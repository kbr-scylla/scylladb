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
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "abstract_function_selector.hh"
#include "cql3/functions/scalar_function.hh"

namespace cql3 {

namespace selection {

class scalar_function_selector : public abstract_function_selector_for<functions::scalar_function> {
public:
    virtual bool is_aggregate() const override {
        // We cannot just return true as it is possible to have a scalar function wrapping an aggregation function
        if (_arg_selectors.empty()) {
            return false;
        }

        return _arg_selectors[0]->is_aggregate();
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(sf, rs);
        }
    }

    virtual void reset() override {
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            _args[i] = s->get_output(sf);
            s->reset();
        }
        return fun()->execute(sf, _args);
    }

    virtual bool requires_thread() const override;

    scalar_function_selector(shared_ptr<functions::function> fun, std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector_for<functions::scalar_function>(
                dynamic_pointer_cast<functions::scalar_function>(std::move(fun)), std::move(arg_selectors)) {
    }
};

}
}
