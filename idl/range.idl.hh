/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

template<typename T>
class range_bound {
    T value();
    bool is_inclusive();
};

template<typename T>
class range {
    std::experimental::optional<range_bound<T>> start();
    std::experimental::optional<range_bound<T>> end();
    bool is_singular();
};

template<typename T>
class nonwrapping_range {
    std::experimental::optional<range_bound<T>> start();
    std::experimental::optional<range_bound<T>> end();
    bool is_singular();
};
