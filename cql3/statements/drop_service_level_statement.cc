/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastarx.hh>
#include "cql3/statements/drop_service_level_statement.hh"
#include "service/qos/service_level_controller.hh"

namespace cql3 {

namespace statements {

drop_service_level_statement::drop_service_level_statement(sstring service_level, bool if_exists) :
    _service_level(service_level), _if_exists(if_exists) {
}

void drop_service_level_statement::validate(service::storage_proxy &, const service::client_state &) const {
}

future<> drop_service_level_statement::check_access(const service::client_state &state) const {
    return state.ensure_has_permission(auth::permission::DROP, auth::root_service_level_resource());
}

future<::shared_ptr<cql_transport::messages::result_message>>
drop_service_level_statement::execute(service::storage_proxy &sp,
        service::query_state &state,
        const query_options &) const {
    return state.get_service_level_controller().drop_distributed_service_level(_service_level, _if_exists).then([] {
        using void_result_msg = cql_transport::messages::result_message::void_message;
        using result_msg = cql_transport::messages::result_message;
        return ::static_pointer_cast<result_msg>(make_shared<void_result_msg>());
    });
}
}
}
