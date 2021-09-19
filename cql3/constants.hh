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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/abstract_marker.hh"
#include "cql3/update_parameters.hh"
#include "cql3/operation.hh"
#include "cql3/values.hh"
#include "cql3/term.hh"
#include "mutation.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {

/**
 * Static helper methods and classes for constants.
 */
class constants {
public:
#if 0
    private static final Logger logger = LoggerFactory.getLogger(Constants.class);
#endif
public:
    /**
    * A constant value, i.e. a ByteBuffer.
    */
    class value : public terminal {
    public:
        cql3::raw_value _bytes;
        value(cql3::raw_value bytes_, data_type my_type) : terminal(std::move(my_type)), _bytes(std::move(bytes_)) {}
        virtual cql3::raw_value get(const query_options& options) override { return _bytes; }
        virtual sstring to_string() const override { return _bytes.to_view().with_value([] (const FragmentedView auto& v) { return to_hex(v); }); }
    };

    static thread_local const ::shared_ptr<value> UNSET_VALUE;

    class null_value final : public value {
    public:
        null_value() : value(cql3::raw_value::make_null(), empty_type) {}
        virtual ::shared_ptr<terminal> bind(const query_options& options) override { return {}; }
        virtual sstring to_string() const override { return "null"; }
    };

    static thread_local const ::shared_ptr<terminal> NULL_VALUE;

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(!_receiver->type->is_collection() && !_receiver->type->is_user_type());
        }

        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            auto bytes = bind_and_get_internal(options);
            if (bytes.is_null()) {
                return ::shared_ptr<terminal>{};
            }
            if (bytes.is_unset_value()) {
                return UNSET_VALUE;
            }
            return ::make_shared<constants::value>(cql3::raw_value::make_value(bytes), _receiver->type);
        }

    private:
        cql3::raw_value_view bind_and_get_internal(const query_options& options) {
            try {
                auto value = options.get_value_at(_bind_index);
                if (value) {
                    value.validate(*_receiver->type, options.get_cql_serialization_format());
                }
                return value;
            } catch (const marshal_exception& e) {
                throw exceptions::invalid_request_exception(
                        format("Exception while binding column {:s}: {:s}", _receiver->name->to_cql_string(), e.what()));
            }
        }
    };

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = expr::evaluate_to_raw_view(_t, params._options);
            execute(m, prefix, params, column, std::move(value));
        }

        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, cql3::raw_value_view value) {
            if (value.is_null()) {
                m.set_cell(prefix, column, params.make_dead_cell());
            } else if (value.is_value()) {
                m.set_cell(prefix, column, params.make_cell(*column.type, value));
            }
        }
    };

    struct adder final : operation {
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = expr::evaluate_to_raw_view(_t, params._options);
            if (value.is_null()) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment");
            } else if (value.is_unset_value()) {
                return;
            }
            auto increment = value.deserialize<int64_t>(*long_type);
            m.set_cell(prefix, column, params.make_counter_update_cell(increment));
        }
    };

    struct subtracter final : operation {
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = expr::evaluate_to_raw_view(_t, params._options);
            if (value.is_null()) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment");
            } else if (value.is_unset_value()) {
                return;
            }
            auto increment = value.deserialize<int64_t>(*long_type);
            if (increment == std::numeric_limits<int64_t>::min()) {
                throw exceptions::invalid_request_exception(format("The negation of {:d} overflows supported counter precision (signed 8 bytes integer)", increment));
            }
            m.set_cell(prefix, column, params.make_counter_update_cell(-increment));
        }
    };

    class deleter : public operation {
    public:
        deleter(const column_definition& column)
            : operation(column, {})
        { }

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
