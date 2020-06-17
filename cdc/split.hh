/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>
#include "schema_fwd.hh"
#include "timestamp.hh"
#include "bytes.hh"
#include <seastar/util/noncopyable_function.hh>

class mutation;

namespace cdc {

bool should_split(const mutation& base_mutation, const schema& base_schema);
void for_each_change(const mutation& base_mutation, const schema_ptr& base_schema,
        seastar::noncopyable_function<void(mutation, api::timestamp_type, bytes, int&)>);

}
