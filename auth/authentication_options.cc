/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "auth/authentication_options.hh"

#include <iostream>

namespace auth {

std::ostream& operator<<(std::ostream& os, authentication_option a) {
    switch (a) {
        case authentication_option::password: os << "PASSWORD"; break;
        case authentication_option::options: os << "OPTIONS"; break;
    }

    return os;
}

}
