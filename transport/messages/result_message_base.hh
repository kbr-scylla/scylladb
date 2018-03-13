
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
public:
    class visitor;
    class visitor_base;

    virtual ~result_message() {}

    virtual void accept(visitor&) = 0;

    //
    // Message types:
    //
    class void_message;
    class set_keyspace;
    class prepared;
    class schema_change;
    class rows;
};

}
}
