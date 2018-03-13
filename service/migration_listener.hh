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

#include <seastar/core/sstring.hh>

namespace service {

class migration_listener {
public:
    virtual ~migration_listener()
    { }

    // The callback runs inside seastar thread
    virtual void on_create_keyspace(const sstring& ks_name) = 0;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) = 0;

    // The callback runs inside seastar thread
    virtual void on_update_keyspace(const sstring& ks_name) = 0;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool columns_changed) = 0;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) = 0;

    // The callback runs inside seastar thread
    virtual void on_drop_keyspace(const sstring& ks_name) = 0;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) = 0;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) = 0;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) = 0;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) = 0;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) = 0;
};

}
