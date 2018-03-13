/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

namespace query {

enum class digest_algorithm : uint8_t {
    none = 0,  // digest not required
    MD5 = 1,   // default algorithm
};

}
