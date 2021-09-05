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
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "audit/audit.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/checked_ptr.hh>
#include <optional>
#include <vector>
#include <memory>

#include "exceptions/exceptions.hh"

namespace cql3 {

class prepare_context;
class column_specification;
class cql_statement;

namespace statements {

struct invalidated_prepared_usage_attempt {
    void operator()() const {
        throw exceptions::invalidated_prepared_usage_attempt_exception();
    }
};

class prepared_statement : public seastar::weakly_referencable<prepared_statement> {
public:
    typedef seastar::checked_ptr<seastar::weak_ptr<prepared_statement>> checked_weak_ptr;

public:
    const seastar::shared_ptr<cql_statement> statement;
    const std::vector<seastar::lw_shared_ptr<column_specification>> bound_names;
    std::vector<uint16_t> partition_key_bind_indices;
    std::vector<sstring> warnings;

    prepared_statement(audit::audit_info_ptr&& audit_info, seastar::shared_ptr<cql_statement> statement_, std::vector<seastar::lw_shared_ptr<column_specification>> bound_names_,
                       std::vector<uint16_t> partition_key_bind_indices, std::vector<sstring> warnings = {});

    prepared_statement(audit::audit_info_ptr&& audit_info, seastar::shared_ptr<cql_statement> statement_, const prepare_context& ctx, const std::vector<uint16_t>& partition_key_bind_indices,
                       std::vector<sstring> warnings = {});

    prepared_statement(audit::audit_info_ptr&& audit_info, seastar::shared_ptr<cql_statement> statement_, prepare_context&& ctx, std::vector<uint16_t>&& partition_key_bind_indices);

    prepared_statement(audit::audit_info_ptr&& audit_info, seastar::shared_ptr<cql_statement>&& statement_);

    checked_weak_ptr checked_weak_from_this() {
        return checked_weak_ptr(this->weak_from_this());
    }
};

}

}
