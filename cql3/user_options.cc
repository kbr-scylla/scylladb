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
 * Modified by ScyllaDB
 *
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <string.h>

#include <boost/range/adaptor/map.hpp>

#include "auth/authenticator.hh"
#include "user_options.hh"

void cql3::user_options::put(const sstring& name, const sstring& value) {
    _options[auth::authenticator::string_to_option(name)] = value;
}

void cql3::user_options::validate(const auth::authenticator& a) const {
    for (auto o : _options | boost::adaptors::map_keys) {
        if (!a.supported_options().contains(o)) {
            throw exceptions::invalid_request_exception(
                            sprint("%s doesn't support %s option",
                                            a.qualified_java_name(),
                                            a.option_to_string(o)));
        }
    }
}

