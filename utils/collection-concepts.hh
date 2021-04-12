/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once
#include <type_traits>
#include <seastar/util/concepts.hh>
#include <compare>

template <typename Func, typename T>
concept Disposer = requires (Func f, T* val) { 
    { f(val) } noexcept -> std::same_as<void>;
};

template <typename Key1, typename Key2, typename Less>
concept LessComparable = requires (const Key1& a, const Key2& b, Less less) {
    { less(a, b) } -> std::same_as<bool>;
    { less(b, a) } -> std::same_as<bool>;
};

template <typename Key1, typename Key2, typename Less>
concept LessNothrowComparable = LessComparable<Key1, Key2, Less> && std::is_nothrow_invocable_v<Less, Key1, Key2>;

// Temporary scaffolding for supporting trichotomic compares
// that return either int or std::strong_ordering
template <typename T>
concept int_or_strong_ordering = std::same_as<T, int> || std::same_as<T, std::strong_ordering>;

template <typename T1, typename T2, typename Compare>
concept Comparable = requires (const T1& a, const T2& b, Compare cmp) {
    // The Comparable is trichotomic comparator that should return 
    //   negative value when a < b
    //   zero when a == b
    //   positive value when a > b
    { cmp(a, b) } -> int_or_strong_ordering;
};