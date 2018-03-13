/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

template <typename ResultType, typename Visitor>
struct boost_visitor_adapter : Visitor {
    using result_type = ResultType;
    boost_visitor_adapter(Visitor&& v) : Visitor(std::move(v)) {}
};

// Boost 1.55 requires that visitors expose a `result_type` member. This
// function adds it.
template <typename ResultType = void, typename Visitor>
auto
to_boost_visitor(Visitor&& v) {
    return boost_visitor_adapter<ResultType, Visitor>(std::move(v));
}
