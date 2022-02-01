/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

template <typename Dividend, typename Divisor>
inline
// requires Integral<Dividend> && Integral<Divisor>
auto
div_ceil(Dividend dividend, Divisor divisor) {
    return (dividend + divisor - 1) / divisor;
}
