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
#include "to_string.hh"
#include "operation.hh"
#include "utils/chunked_vector.hh"

namespace cql3 {

/**
 * Static helper methods and classes for lists.
 */
class lists {
    lists() = delete;
public:
    static lw_shared_ptr<column_specification> index_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> uuid_index_spec_of(const column_specification&);
public:
    class setter : public operation {
    public:
        setter(const column_definition& column, expr::expression e)
                : operation(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const expr::constant& value);
    };

    class setter_by_index : public operation {
    protected:
        expr::expression _idx;
    public:
        setter_by_index(const column_definition& column, expr::expression idx, expr::expression e)
            : operation(column, std::move(e)), _idx(std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void fill_prepare_context(prepare_context& ctx) override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class setter_by_uuid : public setter_by_index {
    public:
        setter_by_uuid(const column_definition& column, expr::expression idx, expr::expression e)
            : setter_by_index(column, std::move(idx), std::move(e)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class appender : public operation {
    public:
        using operation::operation;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    static void do_append(const expr::constant& list_value,
            mutation& m,
            const clustering_key_prefix& prefix,
            const column_definition& column,
            const update_parameters& params);

    class prepender : public operation {
    public:
        using operation::operation;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder : public operation {
    public:
        discarder(const column_definition& column, expr::expression e)
                : operation(column, std::move(e)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder_by_index : public operation {
    public:
        discarder_by_index(const column_definition& column, expr::expression idx)
                : operation(column, std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
