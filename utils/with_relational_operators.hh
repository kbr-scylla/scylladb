/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/gcc6-concepts.hh>
#include <type_traits>

GCC6_CONCEPT(
template<typename T>
concept bool HasTriCompare =
    requires(const T& t) {
        { t.compare(t) } -> int;
    } && std::is_same<std::result_of_t<decltype(&T::compare)(T, T)>, int>::value; //FIXME: #1449
)

template<typename T>
class with_relational_operators {
private:
    template<typename U>
    GCC6_CONCEPT( requires HasTriCompare<U> )
    int do_compare(const U& t) const {
        return static_cast<const U*>(this)->compare(t);
    }
public:
    bool operator<(const T& t) const {
        return do_compare(t) < 0;
    }

    bool operator<=(const T& t) const {
        return do_compare(t) <= 0;
    }

    bool operator>(const T& t) const {
        return do_compare(t) > 0;
    }

    bool operator>=(const T& t) const {
        return do_compare(t) >= 0;
    }

    bool operator==(const T& t) const {
        return do_compare(t) == 0;
    }

    bool operator!=(const T& t) const {
        return do_compare(t) != 0;
    }
};
