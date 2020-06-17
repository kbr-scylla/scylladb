/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <ranges>

namespace ranges {

template <std::ranges::range Container, std::ranges::range Range>
Container to(const Range& range) {
    return Container(range.begin(), range.end());
}

}
