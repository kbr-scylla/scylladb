
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#ifndef SERVICE_QUERY_STATE_HH
#define SERVICE_QUERY_STATE_HH

#include "service/client_state.hh"
#include "tracing/tracing.hh"

namespace service {

class query_state final {
private:
    client_state _client_state;
    tracing::trace_state_ptr _trace_state_ptr;

public:
    query_state(client_state client_state)
        : _client_state(client_state)
        , _trace_state_ptr(_client_state.get_trace_state())
    { }

    const tracing::trace_state_ptr& get_trace_state() const {
        return _trace_state_ptr;
    }

    tracing::trace_state_ptr& get_trace_state() {
        return _trace_state_ptr;
    }

    client_state& get_client_state() {
        return _client_state;
    }

    const client_state& get_client_state() const {
        return _client_state;
    }
    api::timestamp_type get_timestamp() {
        return _client_state.get_timestamp();
    }
};

}

#endif
