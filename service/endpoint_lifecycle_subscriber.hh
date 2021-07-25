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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "gms/inet_address.hh"
#include "utils/atomic_vector.hh"

namespace service {

/**
 * Interface on which interested parties can be notified of high level endpoint
 * state changes.
 *
 * Note that while IEndpointStateChangeSubscriber notify about gossip related
 * changes (IEndpointStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
class endpoint_lifecycle_subscriber {
public:
    virtual ~endpoint_lifecycle_subscriber()
    { }

    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param endpoint the newly added endpoint.
     */
    virtual void on_join_cluster(const gms::inet_address& endpoint) = 0;

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param endpoint the endpoint that is leaving.
     */
    virtual void on_leave_cluster(const gms::inet_address& endpoint) = 0;

    /**
     * Called when a node is marked UP.
     *
     * @param endpoint the endpoint marked UP.
     */
    virtual void on_up(const gms::inet_address& endpoint) = 0;

    /**
     * Called when a node is marked DOWN.
     *
     * @param endpoint the endpoint marked DOWN.
     */
    virtual void on_down(const gms::inet_address& endpoint) = 0;
};

class endpoint_lifecycle_notifier {
    atomic_vector<endpoint_lifecycle_subscriber*> _subscribers;

public:
    void register_subscriber(endpoint_lifecycle_subscriber* subscriber);
    future<> unregister_subscriber(endpoint_lifecycle_subscriber* subscriber) noexcept;

    future<> notify_down(gms::inet_address endpoint);
    future<> notify_left(gms::inet_address endpoint);
    future<> notify_up(gms::inet_address endpoint);
    future<> notify_joined(gms::inet_address endpoint);
};

}
