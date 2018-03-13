/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <memory>
#include <core/future.hh>
#include <core/sharded.hh>

class database;

namespace cql3 {
    class query_processor;
}

namespace db {

class commitlog;

class commitlog_replayer {
public:
    commitlog_replayer(commitlog_replayer&&) noexcept;
    ~commitlog_replayer();

    static future<commitlog_replayer> create_replayer(seastar::sharded<cql3::query_processor>&);

    future<> recover(std::vector<sstring> files);
    future<> recover(sstring file);

private:
    commitlog_replayer(seastar::sharded<cql3::query_processor>&);

    class impl;
    std::unique_ptr<impl> _impl;
};

}
