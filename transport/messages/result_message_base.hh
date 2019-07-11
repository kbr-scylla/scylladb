
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

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

    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class prepared;
    class schema_change;
    class rows;
};

std::ostream& operator<<(std::ostream& os, const result_message& msg);

}
}
