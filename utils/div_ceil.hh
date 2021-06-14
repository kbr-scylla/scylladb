/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

template <typename Dividend, typename Divisor>
inline
// requires Integral<Dividend> && Integral<Divisor>
auto
div_ceil(Dividend dividend, Divisor divisor) {
    return (dividend + divisor - 1) / divisor;
}
