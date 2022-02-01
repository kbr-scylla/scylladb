/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "auth/permissions_cache.hh"

#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "auth/service.hh"

namespace auth {

permissions_cache::permissions_cache(const permissions_cache_config& c, service& ser, logging::logger& log)
        : _cache(c.max_entries, c.validity_period, c.update_period, log, [&ser, &log](const key_type& k) {
              log.debug("Refreshing permissions for {}", k.first);
              return ser.get_uncached_permissions(k.first, k.second);
          }) {
}

future<permission_set> permissions_cache::get(const role_or_anonymous& maybe_role, const resource& r) {
    return do_with(key_type(maybe_role, r), [this](const auto& k) {
        return _cache.get(k);
    });
}

}
