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

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include "seastarx.hh"

namespace auth {

class authenticated_user {
public:
    static const sstring ANONYMOUS_USERNAME;

    authenticated_user();
    authenticated_user(sstring name);
    authenticated_user(authenticated_user&&);
    authenticated_user(const authenticated_user&);

    const sstring& name() const;

    /**
     * If IAuthenticator doesn't require authentication, this method may return true.
     */
    bool is_anonymous() const {
        return _anon;
    }

    bool operator==(const authenticated_user&) const;
private:
    sstring _name;
    bool _anon;
};

}

