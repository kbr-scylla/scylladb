/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

namespace replica {

struct unknown_exception {};

struct no_exception {};

class rate_limit_exception {
};

struct exception_variant {
    std::variant<replica::unknown_exception,
            replica::no_exception,
            replica::rate_limit_exception
    > reason;
};

}
