/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "auth/allow_all_authorizer.hh"

#include "auth/common.hh"
#include "utils/class_registrator.hh"

namespace auth {

const sstring& allow_all_authorizer_name() {
    static const sstring name = make_sstring(meta::AUTH_PACKAGE_NAME, "AllowAllAuthorizer");
    return name;
}

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
    authorizer,
    allow_all_authorizer,
    cql3::query_processor&,
    ::service::migration_manager&> registration("org.apache.cassandra.auth.AllowAllAuthorizer");

}
