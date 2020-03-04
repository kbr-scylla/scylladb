/*
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "query-result.hh"
#include "schema_fwd.hh"
#include "frozen_mutation.hh"

namespace db {
class schema_ctxt;
}

// Transport for schema_ptr across shards/nodes.
// It's safe to access from another shard by const&.
class frozen_schema {
    bytes _data;
public:
    explicit frozen_schema(bytes);
    frozen_schema(const schema_ptr&);
    frozen_schema(frozen_schema&&) = default;
    frozen_schema(const frozen_schema&) = default;
    frozen_schema& operator=(const frozen_schema&) = default;
    frozen_schema& operator=(frozen_schema&&) = default;
    schema_ptr unfreeze(const db::schema_ctxt&) const;
    bytes_view representation() const;
};
