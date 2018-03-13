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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/net/inet_address.hh>
#include "inet_address.hh"

future<gms::inet_address> gms::inet_address::lookup(sstring name) {
    return seastar::net::inet_address::find(name, seastar::net::inet_address::family::INET).then([](seastar::net::inet_address&& a) {
        ::in_addr in = a;
        return make_ready_future<gms::inet_address>(net::ntoh(in.s_addr));
    });
}
