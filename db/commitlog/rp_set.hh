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
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <unordered_map>

#include "replay_position.hh"

namespace db {

class rp_set {
public:
    typedef std::unordered_map<segment_id_type, uint64_t> usage_map;

    rp_set()
    {}
    rp_set(const replay_position & rp)
    {
        put(rp);
    }
    rp_set(rp_set&&) = default;

    rp_set& operator=(rp_set&&) = default;

    void put(const replay_position& rp) {
        _usage[rp.id]++;
    }
    void put(rp_handle && h) {
        if (h) {
            put(h.rp());
        }
        h.release();
    }

    size_t size() const {
        return _usage.size();
    }
    bool empty() const {
        return _usage.empty();
    }

    const usage_map& usage() const {
        return _usage;
    }
private:
    usage_map _usage;
};

}
