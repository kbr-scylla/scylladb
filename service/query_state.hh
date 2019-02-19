
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
//#include "service/qos/service_level_controller.hh"
#include "tracing/tracing.hh"

namespace qos {
class service_level_controller;
}
namespace service {

class query_state final {
private:
    client_state _client_state;
    tracing::trace_state_ptr _trace_state_ptr;
    std::optional<std::reference_wrapper<qos::service_level_controller>> _sl_controller;
public:
    query_state(client_state client_state)
            : _client_state(client_state)
            , _trace_state_ptr(_client_state.get_trace_state())
    {}

    query_state(client_state client_state, qos::service_level_controller &sl_controller)
        : _client_state(client_state)
        , _trace_state_ptr(_client_state.get_trace_state())
        , _sl_controller(sl_controller)
    {}

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

    qos::service_level_controller& get_service_level_controller() const {
        return _sl_controller->get();
    }

};

}

#endif
