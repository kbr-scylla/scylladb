/*
 */
/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "abstract_function_selector.hh"
#include "cql3/functions/aggregate_function.hh"
#include "cql_serialization_format.hh"

#pragma once

namespace cql3 {

namespace selection {

class aggregate_function_selector : public abstract_function_selector_for<functions::aggregate_function> {
    std::unique_ptr<functions::aggregate_function::aggregate> _aggregate;
public:
    virtual bool is_aggregate() const override {
        return true;
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        // Aggregation of aggregation is not supported
        size_t m = _arg_selectors.size();
        for (size_t i = 0; i < m; ++i) {
            auto&& s = _arg_selectors[i];
            s->add_input(sf, rs);
            _args[i] = s->get_output(sf);
            s->reset();
        }
        _aggregate->add_input(sf, _args);
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        return _aggregate->compute(sf);
    }

    virtual void reset() override {
        _aggregate->reset();
    }

    aggregate_function_selector(shared_ptr<functions::function> func,
                std::vector<shared_ptr<selector>> arg_selectors)
            : abstract_function_selector_for<functions::aggregate_function>(
                    dynamic_pointer_cast<functions::aggregate_function>(func), std::move(arg_selectors))
            , _aggregate(fun()->new_aggregate()) {
    }
};

}
}
