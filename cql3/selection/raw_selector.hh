/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
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
