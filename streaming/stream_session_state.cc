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
 * Modified by ScyllaDB.
 * Copyright 2015-present ScyllaDB.
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "streaming/stream_session_state.hh"
#include <ostream>
#include <map>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace streaming {

static const std::map<stream_session_state, sstring> stream_session_state_names = {
    {stream_session_state::INITIALIZED,     "INITIALIZED"},
    {stream_session_state::PREPARING,       "PREPARING"},
    {stream_session_state::STREAMING,       "STREAMING"},
    {stream_session_state::WAIT_COMPLETE,   "WAIT_COMPLETE"},
    {stream_session_state::COMPLETE,        "COMPLETE"},
    {stream_session_state::FAILED,          "FAILED"},
};

std::ostream& operator<<(std::ostream& os, const stream_session_state& s) {
    os << stream_session_state_names.at(s);
    return os;
}

}
