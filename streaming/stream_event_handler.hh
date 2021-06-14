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

#include "streaming/stream_event.hh"

namespace streaming {

class stream_event_handler /* extends FutureCallback<StreamState> */ {
public:
    /**
     * Callback for various streaming events.
     *
     * @see StreamEvent.Type
     * @param event Stream event.
     */
    virtual void handle_stream_event(session_complete_event event) {}
    virtual void handle_stream_event(progress_event event) {}
    virtual void handle_stream_event(session_prepared_event event) {}
    virtual ~stream_event_handler() {};
};

} // namespace streaming
