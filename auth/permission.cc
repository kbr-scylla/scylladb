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

#include "auth/permission.hh"

#include <boost/algorithm/string.hpp>

#include <unordered_map>

const auth::permission_set auth::permissions::ALL = auth::permission_set::of<
        auth::permission::CREATE,
        auth::permission::ALTER,
        auth::permission::DROP,
        auth::permission::SELECT,
        auth::permission::MODIFY,
        auth::permission::AUTHORIZE,
        auth::permission::DESCRIBE>();

const auth::permission_set auth::permissions::NONE;

static const std::unordered_map<sstring, auth::permission> permission_names({
        {"READ", auth::permission::READ},
        {"WRITE", auth::permission::WRITE},
        {"CREATE", auth::permission::CREATE},
        {"ALTER", auth::permission::ALTER},
        {"DROP", auth::permission::DROP},
        {"SELECT", auth::permission::SELECT},
        {"MODIFY", auth::permission::MODIFY},
        {"AUTHORIZE", auth::permission::AUTHORIZE},
        {"DESCRIBE", auth::permission::DESCRIBE}});

const sstring& auth::permissions::to_string(permission p) {
    for (auto& v : permission_names) {
        if (v.second == p) {
            return v.first;
        }
    }
    throw std::out_of_range("unknown permission");
}

auth::permission auth::permissions::from_string(const sstring& s) {
    sstring upper(s);
    boost::to_upper(upper);
    return permission_names.at(upper);
}

std::unordered_set<sstring> auth::permissions::to_strings(const permission_set& set) {
    std::unordered_set<sstring> res;
    for (auto& v : permission_names) {
        if (set.contains(v.second)) {
            res.emplace(v.first);
        }
    }
    return res;
}

auth::permission_set auth::permissions::from_strings(const std::unordered_set<sstring>& set) {
    permission_set res = auth::permissions::NONE;
    for (auto& s : set) {
        res.set(from_string(s));
    }
    return res;
}

bool auth::operator<(const permission_set& p1, const permission_set& p2) {
    return p1.mask() < p2.mask();
}
