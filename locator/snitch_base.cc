/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "locator/snitch_base.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"

namespace locator {

std::list<std::pair<gms::application_state, gms::versioned_value>> snitch_base::get_app_states() const {
    return {
        {gms::application_state::DC, gms::versioned_value::datacenter(_my_dc)},
        {gms::application_state::RACK, gms::versioned_value::rack(_my_rack)},
    };
}

snitch_ptr::snitch_ptr(const snitch_config cfg, sharded<gms::gossiper>& g)
        : _gossiper(g) {
    i_endpoint_snitch::ptr_type s;
    try {
        s = create_object<i_endpoint_snitch>(cfg.name, cfg);
    } catch (no_such_class& e) {
        i_endpoint_snitch::logger().error("Can't create snitch {}: not supported", cfg.name);
        throw;
    } catch (...) {
        throw;
    }
    s->set_backreference(*this);
    _ptr = std::move(s);
}

} // namespace locator
