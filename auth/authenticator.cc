/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#include "auth/authenticator.hh"

#include "auth/authenticated_user.hh"
#include "auth/common.hh"
#include "auth/password_authenticator.hh"
#include "cql3/query_processor.hh"
#include "utils/class_registrator.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");
const sstring auth::authenticator::SERVICE_KEY("service");
const sstring auth::authenticator::REALM_KEY("realm");
