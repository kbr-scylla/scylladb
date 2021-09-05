/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

#include "cql3/selection/selectable.hh"
#include "cql3/selection/selectable-expr.hh"
#include "cql3/expr/expression.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class raw_selector {
public:
    const expr::expression selectable_;
    const ::shared_ptr<column_identifier> alias;

    raw_selector(expr::expression selectable__, shared_ptr<column_identifier> alias_)
        : selectable_{std::move(selectable__)}
        , alias{alias_}
    { }

    /**
     * Converts the specified list of <code>RawSelector</code>s into a list of <code>Selectable</code>s.
     *
     * @param raws the <code>RawSelector</code>s to converts.
     * @return a list of <code>Selectable</code>s
     */
    static std::vector<::shared_ptr<selectable>> to_selectables(const std::vector<::shared_ptr<raw_selector>>& raws,
            const schema& schema) {
        std::vector<::shared_ptr<selectable>> r;
        r.reserve(raws.size());
        for (auto&& raw : raws) {
            r.emplace_back(prepare_selectable(schema, raw->selectable_));
        }
        return r;
    }

    bool processes_selection() const {
        return selectable_processes_selection(selectable_);
    }
};

}

}
