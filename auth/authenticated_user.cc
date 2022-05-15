/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "auth/authenticated_user.hh"

#include <iostream>

namespace auth {

authenticated_user::authenticated_user(std::string_view name)
        : name(sstring(name)) {
}

std::ostream& operator<<(std::ostream& os, const authenticated_user& u) {
    if (!u.name) {
        os << "anonymous";
    } else {
        os << *u.name;
    }

    return os;
}

static const authenticated_user the_anonymous_user{};

const authenticated_user& anonymous_user() noexcept {
    return the_anonymous_user;
}

}
