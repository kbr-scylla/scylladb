/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
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
