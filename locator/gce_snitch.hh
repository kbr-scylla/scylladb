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
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2018-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"
#include <seastar/http/response_parser.hh>

namespace locator {

class gce_snitch : public production_snitch_base {
public:
    static constexpr const char* ZONE_NAME_QUERY_REQ = "/computeMetadata/v1/instance/zone";
    static constexpr const char* GCE_QUERY_SERVER_ADDR = "metadata.google.internal";

    explicit gce_snitch(const sstring& fname = "", unsigned io_cpu_id = 0, const sstring& meta_server_url = "");
    virtual future<> start() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.GoogleCloudSnitch";
    }
protected:
    future<> load_config();
    future<sstring> gce_api_call(sstring addr, const sstring cmd);
    future<sstring> read_property_file();

private:
    sstring _meta_server_url;
};

} // namespace locator
