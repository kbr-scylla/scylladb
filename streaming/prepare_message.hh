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
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "streaming/stream_request.hh"
#include "streaming/stream_summary.hh"

namespace streaming {

class prepare_message {
public:
    /**
     * Streaming requests
     */
    std::vector<stream_request> requests;

    /**
     * Summaries of streaming out
     */
    std::vector<stream_summary> summaries;

    uint32_t dst_cpu_id;

    prepare_message() = default;
    prepare_message(std::vector<stream_request> reqs, std::vector<stream_summary> sums, uint32_t dst_cpu_id_ = -1)
        : requests(std::move(reqs))
        , summaries(std::move(sums))
        , dst_cpu_id(dst_cpu_id_) {
    }
};

} // namespace streaming
