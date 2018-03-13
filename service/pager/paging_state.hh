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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <experimental/optional>

#include "bytes.hh"
#include "keys.hh"

namespace service {

namespace pager {

class paging_state final {
    partition_key _partition_key;
    std::experimental::optional<clustering_key> _clustering_key;
    uint32_t _remaining;

public:
    paging_state(partition_key pk, std::experimental::optional<clustering_key> ck, uint32_t rem);

    /**
     * Last processed key, i.e. where to start from in next paging round
     */
    const partition_key& get_partition_key() const {
        return _partition_key;
    }
    /**
     * Clustering key in last partition. I.e. first, next, row
     */
    const std::experimental::optional<clustering_key>& get_clustering_key() const {
        return _clustering_key;
    }
    /**
     * Max remaining rows to fetch in total.
     * I.e. initial row_limit - #rows returned so far.
     */
    uint32_t get_remaining() const {
        return _remaining;
    }

    static ::shared_ptr<paging_state> deserialize(bytes_opt bytes);
    bytes_opt serialize() const;
};

}

}
