/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <utility>
#include "utils/fmt-compat.hh"

namespace thrift {

template <typename Ex, typename... Args>
Ex
make_exception(const char* fmt, Args&&... args) {
    Ex ex;
    ex.why = fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...);
    return ex;
}

}
