/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <experimental/optional>

template<typename T>
inline
std::experimental::optional<T>
move_and_disengage(std::experimental::optional<T>& opt) {
    auto t = std::move(opt);
    opt = std::experimental::nullopt;
    return t;
}
