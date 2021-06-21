/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>
#include <compare>
#include <boost/test/unit_test.hpp>
#include <seastar/util/variant_utils.hh>
#include <variant>
#include "test/lib/log.hh"
#include "utils/collection-concepts.hh"
#include "to_string.hh"

template<typename Comparator, typename... T>
class total_order_check {
    using element = std::variant<T...>;
    std::vector<std::vector<element>> _data;
    Comparator _cmp;
private:
    // All in left are expect to compare with all in right with expected_order result
    void check_order(const std::vector<element>& left, const std::vector<element>& right, std::strong_ordering order) {
        for (const element& left_e : left) {
            for (const element& right_e : right) {
                seastar::visit(left_e, [&] (auto&& a) {
                    seastar::visit(right_e, [&] (auto&& b) {
                        testlog.trace("cmp({}, {}) == {}", a, b, order);
                        auto r = _cmp(a, b);
                        auto actual = r <=> 0;
                        if (actual != order) {
                            BOOST_FAIL(format("Expected cmp({}, {}) == {}, but got {}", a, b, order, actual));
                        }
                    });
                });
            }
        }
    }
public:
    total_order_check(Comparator c = Comparator()) : _cmp(std::move(c)) {}

    total_order_check& next(element e) {
        _data.push_back(std::vector<element>());
        return equal_to(e);
    }

    total_order_check& equal_to(element e) {
        _data.back().push_back(e);
        return *this;
    }

    void check() {
        for (unsigned i = 0; i < _data.size(); ++i) {
            for (unsigned j = 0; j < _data.size(); ++j) {
                check_order(_data[i], _data[j], i <=> j);
            }
        }
    }
};
