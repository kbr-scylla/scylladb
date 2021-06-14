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

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace db {
namespace index {

/**
 * Abstract base class for different types of secondary indexes.
 *
 * Do not extend this directly, please pick from PerColumnSecondaryIndex or PerRowSecondaryIndex
 */
class secondary_index {
public:
    static const sstring custom_index_option_name;

    /**
     * The name of the option used to specify that the index is on the collection keys.
     */
    static const sstring index_keys_option_name;

    /**
     * The name of the option used to specify that the index is on the collection values.
     */
    static const sstring index_values_option_name;

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    static const sstring index_entries_option_name;

};

}
}
