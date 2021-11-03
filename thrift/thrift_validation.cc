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

#include "thrift_validation.hh"
#include "thrift/utils.hh"
#include "db/system_keyspace.hh"
#include <regex>
#include "utils/fb_utilities.hh"

using namespace thrift;
using namespace ::apache::thrift;

namespace thrift_validation {

void validate_key(const schema& s, const bytes_view& k) {
    if (k.empty()) {
        throw make_exception<InvalidRequestException>("Key may not be empty");
    }

    auto max = static_cast<uint32_t>(utils::fb_utilities::MAX_UNSIGNED_SHORT);
    if (k.size() > max) {
        throw make_exception<InvalidRequestException>("Key length of {} is longer than maximum of {}", k.size(), max);
    }

    // FIXME: implement
    //s.partition_key_type()->validate(k);
}

void validate_keyspace_not_system(const std::string& keyspace) {
    std::string name;
    name.resize(keyspace.length());
    std::transform(keyspace.begin(), keyspace.end(), name.begin(), ::tolower);
    if (is_system_keyspace(name)) {
        throw make_exception<InvalidRequestException>("system keyspace is not user-modifiable");
    }
}

void validate_ks_def(const KsDef& ks_def) {
    validate_keyspace_not_system(ks_def.name);
    std::regex name_regex("\\w+");
    if (!std::regex_match(ks_def.name, name_regex)) {
        throw make_exception<InvalidRequestException>("\"{}\" is not a valid keyspace name", ks_def.name);
    }
    if (ks_def.name.length() > schema::NAME_LENGTH) {
        throw make_exception<InvalidRequestException>("Keyspace names shouldn't be more than {} characters long (got \"{}\")", schema::NAME_LENGTH, ks_def.name);
    }
}

void validate_cf_def(const CfDef& cf_def) {
    std::regex name_regex("\\w+");
    if (!std::regex_match(cf_def.name, name_regex)) {
        throw make_exception<InvalidRequestException>("\"{}\" is not a valid column family name", cf_def.name);
    }
    if (cf_def.name.length() > schema::NAME_LENGTH) {
        throw make_exception<InvalidRequestException>("Keyspace names shouldn't be more than {} characters long (got \"{}\")", schema::NAME_LENGTH, cf_def.name);
    }
}

void validate_column_name(const std::string& name) {
    auto max_name_length = static_cast<uint32_t>(utils::fb_utilities::MAX_UNSIGNED_SHORT);
    if (name.size() > max_name_length) {
        throw make_exception<InvalidRequestException>("column name length must not be greater than {}", max_name_length);
    }
    if (name.empty()) {
        throw make_exception<InvalidRequestException>("column name must not be empty");
    }
}

void validate_column_names(const std::vector<std::string>& names) {
    for (auto&& name : names) {
        validate_column_name(name);
    }
}

void validate_column(const Column& col, const column_definition& def) {
    if (!col.__isset.value) {
        throw make_exception<InvalidRequestException>("Column value is required");
    }
    if (!col.__isset.timestamp) {
        throw make_exception<InvalidRequestException>("Column timestamp is required");
    }
    def.type->validate(to_bytes_view(col.value), cql_serialization_format::latest());
}

}
