
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace cql_transport {
namespace messages {

class result_message {
    std::vector<sstring> _warnings;
public:
    class visitor;
    class visitor_base;

    virtual ~result_message() {}

    virtual void accept(visitor&) const = 0;

    void add_warning(sstring w) {
        _warnings.push_back(std::move(w));
    }

    const std::vector<sstring>& warnings() const {
        return _warnings;
    }

    virtual std::optional<unsigned> move_to_shard() const {
        return std::nullopt;
    }
    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class prepared;
    class schema_change;
    class rows;
    class bounce_to_shard;
};

std::ostream& operator<<(std::ostream& os, const result_message& msg);

}
}
