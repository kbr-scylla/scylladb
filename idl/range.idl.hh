/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

template<typename T>
class range_bound {
    T value();
    bool is_inclusive();
};

template<typename T>
class range {
    std::optional<range_bound<T>> start();
    std::optional<range_bound<T>> end();
    bool is_singular();
};

template<typename T>
class nonwrapping_range {
    std::optional<range_bound<T>> start();
    std::optional<range_bound<T>> end();
    bool is_singular();
};
