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
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include "authenticated_user.hh"

const sstring auth::authenticated_user::ANONYMOUS_USERNAME("anonymous");

auth::authenticated_user::authenticated_user()
                : _anon(true)
{}

auth::authenticated_user::authenticated_user(sstring name)
                : _name(name), _anon(false)
{}

auth::authenticated_user::authenticated_user(authenticated_user&&) = default;
auth::authenticated_user::authenticated_user(const authenticated_user&) = default;

const sstring& auth::authenticated_user::name() const {
    return _anon ? ANONYMOUS_USERNAME : _name;
}

bool auth::authenticated_user::operator==(const authenticated_user& v) const {
    return _anon ? v._anon : _name == v._name;
}
