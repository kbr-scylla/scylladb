/*
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <type_traits>
#include <memory>
#include <seastar/core/future.hh>

#include "seastarx.hh"

#pragma once

//
// Utility for adapting types which are not nothrow move constructible into such
// by wrapping them if necessary.
//
// Example usage:
//
//   T val{};
//   using traits = noexcept_movable<T>;
//   auto f = make_ready_future<typename traits::type>(traits::wrap(std::move(val)));
//   T val2 = traits::unwrap(f.get0());
//

template<typename T, typename Enable = void>
struct noexcept_movable;

template<typename T>
struct noexcept_movable<T, std::enable_if_t<std::is_nothrow_move_constructible<T>::value>> {
    using type = T;

    static type wrap(T&& v) {
        return std::move(v);
    }

    static future<T> wrap(future<T>&& v) {
        return std::move(v);
    }

    static T unwrap(type&& v) {
        return std::move(v);
    }

    static future<T> unwrap(future<type>&& v) {
        return std::move(v);
    }
};

template<typename T>
struct noexcept_movable<T, std::enable_if_t<!std::is_nothrow_move_constructible<T>::value>> {
    using type = std::unique_ptr<T>;

    static type wrap(T&& v) {
        return std::make_unique<T>(std::move(v));
    }

    static T unwrap(type&& v) {
        return std::move(*v);
    }
};

template<typename T>
using noexcept_movable_t = typename noexcept_movable<T>::type;
