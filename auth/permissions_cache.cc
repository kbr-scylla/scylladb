/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "auth/permissions_cache.hh"

#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "auth/service.hh"
#include "db/config.hh"

namespace auth {

permissions_cache_config permissions_cache_config::from_db_config(const db::config& dc) {
    permissions_cache_config c;
    c.max_entries = dc.permissions_cache_max_entries();
    c.validity_period = std::chrono::milliseconds(dc.permissions_validity_in_ms());
    c.update_period = std::chrono::milliseconds(dc.permissions_update_interval_in_ms());

    return c;
}

permissions_cache::permissions_cache(const permissions_cache_config& c, service& ser, logging::logger& log)
        : _cache(c.max_entries, c.validity_period, c.update_period, log, [&ser, &log](const key_type& k) {
              log.debug("Refreshing permissions for {}", k.first.name());
              return ser.underlying_authorizer().authorize(ser, ::make_shared<authenticated_user>(k.first), k.second);
          }) {
}

future<permission_set> permissions_cache::get(::shared_ptr<authenticated_user> user, data_resource r) {
    return _cache.get(key_type(*user, r));
}

}
