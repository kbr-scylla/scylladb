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

#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include "query-request.hh"
#include "dht/i_partitioner.hh"
#include "streaming/stream_coordinator.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_reason.hh"
#include <vector>

namespace streaming {

class stream_state;
class stream_event_handler;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
class stream_plan {
private:
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    using token = dht::token;
    stream_manager& _mgr;
    UUID _plan_id;
    sstring _description;
    stream_reason _reason;
    std::vector<stream_event_handler*> _handlers;
    shared_ptr<stream_coordinator> _coordinator;
    bool _range_added = false;
    bool _aborted = false;
public:

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    stream_plan(stream_manager& mgr, sstring description, stream_reason reason = stream_reason::unspecified)
        : _mgr(mgr)
        , _plan_id(utils::UUID_gen::get_time_UUID())
        , _description(description)
        , _reason(reason)
        , _coordinator(make_shared<stream_coordinator>())
    {
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    stream_plan& request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges);

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    stream_plan& request_ranges(inet_address from, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families);

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    stream_plan& transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges);

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    stream_plan& transfer_ranges(inet_address to, sstring keyspace, dht::token_range_vector ranges, std::vector<sstring> column_families);

    stream_plan& listeners(std::vector<stream_event_handler*> handlers);
public:
    /**
     * @return true if this plan has no plan to execute
     */
    bool is_empty() const {
        return !_coordinator->has_active_sessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    future<stream_state> execute();

    void abort() noexcept;
    void do_abort();
};

} // namespace streaming
