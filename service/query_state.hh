
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
#include "service_permit.hh"

namespace qos {
class service_level_controller;
}
namespace service {

class query_state final {
private:
    client_state& _client_state;
    tracing::trace_state_ptr _trace_state_ptr;
    service_permit _permit;
    std::optional<std::reference_wrapper<qos::service_level_controller>> _sl_controller;

public:
    query_state(client_state& client_state, service_permit permit)
            : _client_state(client_state)
            , _trace_state_ptr(_client_state.get_trace_state())
            , _permit(std::move(permit))
    {}

    query_state(client_state& client_state, service_permit permit, qos::service_level_controller &sl_controller)
        : _client_state(client_state)
        , _trace_state_ptr(_client_state.get_trace_state())
        , _permit(std::move(permit))
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

    service_permit get_permit() const& {
        return _permit;
    }

    service_permit&& get_permit() && {
        return std::move(_permit);
    }

    qos::service_level_controller& get_service_level_controller() const {
        return _sl_controller->get();
    }
};

}

#endif
