/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <utility>

namespace thrift {

template <typename Ex, typename... Args>
Ex
make_exception(const char* fmt, Args&&... args) {
    Ex ex;
    ex.why = fmt::format(fmt, std::forward<Args>(args)...);
    return ex;
}

}
