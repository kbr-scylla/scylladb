/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <stdexcept>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

class marshal_exception : public std::exception {
    sstring _why;
public:
    marshal_exception() = delete;
    marshal_exception(sstring why) : _why(sstring("marshaling error: ") + why) {}
    virtual const char* what() const noexcept override { return _why.c_str(); }
};

// Speed up compilation of code using throw_with_backtrace<marshal_exception,
// sstring> by compiling it only once (in types.cc), and elsewhere say that
// it is extern and not compile it again.
namespace seastar {
template <class Exc, typename... Args> [[noreturn]] void throw_with_backtrace(Args&&... args);
extern template void throw_with_backtrace<marshal_exception, sstring>(sstring&&);
}
