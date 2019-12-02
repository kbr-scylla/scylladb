/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <boost/multiprecision/cpp_int.hpp>
#include <ostream>

#include "bytes.hh"

class big_decimal {
private:
    int32_t _scale;
    boost::multiprecision::cpp_int _unscaled_value;
public:
    enum class rounding_mode {
        HALF_EVEN,
    };

    explicit big_decimal(sstring_view text);
    big_decimal() : big_decimal(0, 0) {}
    big_decimal(int32_t scale, boost::multiprecision::cpp_int unscaled_value)
        : _scale(scale), _unscaled_value(unscaled_value)
    { }

    int32_t scale() const { return _scale; }
    const boost::multiprecision::cpp_int& unscaled_value() const { return _unscaled_value; }

    sstring to_string() const;

    int compare(const big_decimal& other) const;

    big_decimal& operator+=(const big_decimal& other);
    big_decimal& operator-=(const big_decimal& other);
    big_decimal operator+(const big_decimal& other) const;
    big_decimal operator-(const big_decimal& other) const;
    big_decimal div(const ::uint64_t y, const rounding_mode mode) const;
    friend bool operator<(const big_decimal& x, const big_decimal& y) { return x.compare(y) < 0; }
    friend bool operator<=(const big_decimal& x, const big_decimal& y) { return x.compare(y) <= 0; }
    friend bool operator==(const big_decimal& x, const big_decimal& y) { return x.compare(y) == 0; }
    friend bool operator!=(const big_decimal& x, const big_decimal& y) { return x.compare(y) != 0; }
    friend bool operator>=(const big_decimal& x, const big_decimal& y) { return x.compare(y) >= 0; }
    friend bool operator>(const big_decimal& x, const big_decimal& y) { return x.compare(y) > 0; }
};

inline std::ostream& operator<<(std::ostream& s, const big_decimal& v) {
    return s << v.to_string();
}
