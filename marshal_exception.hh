/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <stdexcept>

class marshal_exception : public std::exception {
    sstring _why;
public:
    marshal_exception() : _why("marshalling error") {}
    marshal_exception(sstring why) : _why(sstring("marshaling error: ") + why) {}
    virtual const char* what() const noexcept override { return _why.c_str(); }
};

