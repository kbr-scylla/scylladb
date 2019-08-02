/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#define LDAP_DEPRECATED 1

#include <seastar/testing/thread_test_case.hh>

#include <string>

extern "C" {
#include <ldap.h>
}

// Tests default (non-custom) libber networking.  Failure here indicates a likely bug in test.py's
// LDAP setup.
SEASTAR_THREAD_TEST_CASE(bind_with_default_io) {
    const char* port = "389";

    if (char *p = std::getenv("SEASTAR_LDAP_PORT"); p != nullptr) {
        port = p;
    }

    static constexpr char const *base_dn = "dc=example,dc=com";
    static const std::string server_uri{"ldap://localhost:" + std::string(port)};
    static constexpr const char* manager_dn = "cn=root,dc=example,dc=com";
    static constexpr const char* manager_password = "secret";

    ldap_set_option(nullptr, LDAP_OPT_DEFBASE, base_dn);
    const int protocol_version = LDAP_VERSION3;
    ldap_set_option(nullptr, LDAP_OPT_PROTOCOL_VERSION, &protocol_version);
    ldap *manager_client_state{nullptr};
    BOOST_REQUIRE_EQUAL(ldap_initialize(&manager_client_state, server_uri.c_str()), 0);
    // Retry on EINTR, rather than return error:
    BOOST_REQUIRE_EQUAL(LDAP_OPT_SUCCESS, ldap_set_option(manager_client_state, LDAP_OPT_RESTART, LDAP_OPT_ON));
    BOOST_REQUIRE_EQUAL(ldap_simple_bind_s(manager_client_state, manager_dn, manager_password), 0);
}
