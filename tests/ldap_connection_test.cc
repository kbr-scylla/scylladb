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

#include <cassert>
#include <exception>
#include <set>
#include <string>

#include <ent/ldap/ldap_connection.hh>

#include "seastarx.hh"

extern "C" {
#include <ldap.h>
}

namespace {

logger mylog{"ldap_connection_test"}; // `log` is taken by math.

constexpr auto base_dn = "dc=example,dc=com";
constexpr auto manager_dn = "cn=root,dc=example,dc=com";
constexpr auto manager_password = "secret";
const auto ldap_envport = std::getenv("SEASTAR_LDAP_PORT");
const std::string ldap_port(ldap_envport ? ldap_envport : "389");
const socket_address local_ldap_address(ipv4_addr("127.0.0.1", std::stoi(ldap_port)));
const socket_address local_fail_inject_address(ipv4_addr("127.0.0.1", std::stoi(ldap_port) + 2));
const int globally_set_search_dn_before_any_test_runs = ldap_set_option(nullptr, LDAP_OPT_DEFBASE, base_dn);
const std::set<std::string> results_expected_from_search_base_dn{
    "dc=example,dc=com",
    "cn=root,dc=example,dc=com",
    "ou=People,dc=example,dc=com",
    "uid=cassandra,ou=People,dc=example,dc=com",
    "uid=jsmith,ou=People,dc=example,dc=com"
};

/// Functor to invoke ldap_msgfree.
struct msg_deleter {
    void operator()(LDAPMessage* p) {
        ldap_msgfree(p);
    }
};

using msg_ptr = std::unique_ptr<LDAPMessage, msg_deleter>;

/// Thrown when ldap_result returns an unexpected error value.
struct result_exception : public std::runtime_error {
    result_exception(const char* what) : std::runtime_error(what) {}
};

/// Gets an LDAP result indicated by msgid.  Captures \p conn, which must survive until the returned future resolves.
future<msg_ptr> result(const ldap_connection& conn, int msgid) {
    mylog.trace("result: msgid={}", msgid);
    // Busy-wait on the result.  This is just for testing that ordinary ldap_* functions work as
    // expected with custom Seastar LDAP.  In production, a different waiting loop is used.
    return repeat_until_value([&conn, msgid] {
        LDAPMessage *result;
        timeval zero_duration{}; // Must allow Seastar to run other fibers to complete networking operations.
        mylog.trace("Invoking ldap_result for msgid={}", msgid);
        const auto result_type = ldap_result(conn.get_ldap(), msgid, /*all=*/0, &zero_duration, &result);
        mylog.debug("ldap_result for msgid={} returned {}", msgid, result_type);
        if (result_type < 0) {
            mylog.error("ldap_result error: {}", conn.get_error());
            throw result_exception("negative ldap_result");
        }
        return make_ready_future<compat::optional<msg_ptr>>(
                result_type ? compat::make_optional(msg_ptr(result)) : compat::nullopt);
    });
}

/// Attempts to get msgid's LDAP result, but ignores errors.  Useful for failure-injection tests,
/// which must tolerate communication failure.
void try_get_result(const ldap_connection& conn, int msgid) {
    try {
        result(conn, msgid).handle_exception([] (std::exception_ptr) { return msg_ptr(); }).get();
    } catch (result_exception&) {
        // Swallow.
    }
}

/// Extracts n entries resulting from a prior ldap_search operation msgid.
std::set<std::string> entries(size_t n, const ldap_connection& conn, int msgid) {
    std::set<std::string> retval;
    auto ld = conn.get_ldap();
    for (size_t i = 0; i < n; ++i) {
        const auto res = result(conn, msgid).get0();
        BOOST_REQUIRE_MESSAGE(ldap_msgtype(res.get()) == LDAP_RES_SEARCH_ENTRY, i);
        for (auto e = ldap_first_entry(ld, res.get()); e; e = ldap_next_entry(ld, e)) {
            char* dn = ldap_get_dn(ld, e);
            retval.insert(dn);
            ldap_memfree(dn);
        }
    }
    return retval;
}

int search(const ldap_connection& conn, const char* term, int* msgid) {
    // Can't use synchronous LDAP operations, since they stall the Seastar reactor.
    return ldap_search_ext(
            conn.get_ldap(),
            term,
            LDAP_SCOPE_SUBTREE,
            /*filter=*/nullptr,
            /*attrs=*/nullptr,
            /*attrsonly=*/0,
            /*serverctrls=*/nullptr,
            /*clientctrls=*/nullptr,
            /*timeout=*/nullptr,
            /*sizelimit=*/0,
            msgid);
}

int bind(ldap_connection& conn)
{
    // Can't use synchronous LDAP operations, since they stall the Seastar reactor.
    return ldap_simple_bind(conn.get_ldap(), manager_dn, manager_password);
}

} // anonymous namespace

// Tests default (non-custom) libber networking.  Failure here indicates a likely bug in test.py's
// LDAP setup.
SEASTAR_THREAD_TEST_CASE(bind_with_default_io) {
    const auto server_uri = "ldap://localhost:" + ldap_port;
    LDAP *manager_client_state{nullptr};
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_initialize(&manager_client_state, server_uri.c_str()));
    static constexpr int v3 = LDAP_VERSION3;
    BOOST_REQUIRE_EQUAL(LDAP_OPT_SUCCESS, ldap_set_option(manager_client_state, LDAP_OPT_PROTOCOL_VERSION, &v3));
    // Retry on EINTR, rather than return error:
    BOOST_REQUIRE_EQUAL(LDAP_OPT_SUCCESS, ldap_set_option(manager_client_state, LDAP_OPT_RESTART, LDAP_OPT_ON));
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_simple_bind_s(manager_client_state, manager_dn, manager_password));
    BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, ldap_unbind(manager_client_state));
}

SEASTAR_THREAD_TEST_CASE(bind_with_custom_sockbuf_io) {
    mylog.trace("bind_with_custom_sockbuf_io");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        mylog.trace("bind_with_custom_sockbuf_io invoking bind");
        const auto id = bind(c);
        mylog.trace("bind_with_custom_sockbuf_io bind returned {}", id);
        BOOST_REQUIRE_NE(id, -1);
        // Await the bind result:
        auto r = result(c, id).get0();
        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(r.get()));
    });
    mylog.trace("bind_with_custom_sockbuf_io done");
}

SEASTAR_THREAD_TEST_CASE(search_with_custom_sockbuf_io) {
    mylog.trace("search_with_custom_sockbuf_io");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        int msgid;
        mylog.trace("search_with_custom_sockbuf_io: invoking ldap search");
        BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, search(c, base_dn, &msgid));
        mylog.trace("search_with_custom_sockbuf_io: ldap search returned msgid {}", msgid);
        const std::set<std::string>& expected = results_expected_from_search_base_dn;
        const auto actual = entries(expected.size(), c, msgid);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.cbegin(), actual.cend(), expected.cbegin(), expected.cend());
    });
    mylog.trace("search_with_custom_sockbuf_io done");
}

SEASTAR_THREAD_TEST_CASE(multiple_outstanding_operations) {
    mylog.trace("multiple_outstanding_operations");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        const auto id_bind = bind(c);
        mylog.trace("bind_with_custom_sockbuf_io bind returned {}", id_bind);
        BOOST_REQUIRE_NE(id_bind, -1);

        int id_search_base;
        mylog.trace("multiple_outstanding_operations: invoking search base");
        BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, search(c, base_dn, &id_search_base));
        mylog.trace("multiple_outstanding_operations: search base returned msgid {}", id_search_base);

        int id_search_jsmith;
        mylog.trace("multiple_outstanding_operations: invoking search jsmith");
        BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, search(c, "uid=jsmith,ou=People,dc=example,dc=com", &id_search_jsmith));
        mylog.trace("multiple_outstanding_operations: search jsmith returned msgid {}", id_search_jsmith);

        const auto actual_for_base = entries(results_expected_from_search_base_dn.size(), c, id_search_base);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
                actual_for_base.cbegin(), actual_for_base.cend(),
                results_expected_from_search_base_dn.cbegin(), results_expected_from_search_base_dn.cend());

        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(result(c, id_bind).get0().get()));

        const std::set<std::string> expected_for_jsmith{"uid=jsmith,ou=People,dc=example,dc=com"};
        const auto actual_for_jsmith = entries(expected_for_jsmith.size(), c, id_search_jsmith);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
                actual_for_jsmith.cbegin(), actual_for_jsmith.cend(),
                expected_for_jsmith.cbegin(), expected_for_jsmith.cend());
    });
    mylog.trace("multiple_outstanding_operations done");
}

SEASTAR_THREAD_TEST_CASE(early_shutdown) {
    mylog.trace("early_shutdown: noop");
    with_ldap_connection(local_ldap_address, [] (ldap_connection&) {});
    mylog.trace("early_shutdown: bind");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) { bind(c); });
    mylog.trace("early_shutdown: search");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        int dummy_id;
        search(c, base_dn, &dummy_id);
    });
}

SEASTAR_THREAD_TEST_CASE(bind_after_fail) {
    mylog.trace("bind_after_fail: wonky connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& wonky_conn) {
        const auto id = bind(wonky_conn);
        if (id != -1) {
            try_get_result(wonky_conn, id);
        }
    });
    mylog.trace("bind_after_fail: solid connection");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        const auto id = bind(c);
        BOOST_REQUIRE_NE(id, -1);
        mylog.trace("bind_after_fail: bind returned {}", id);
        // Await the bind result:
        auto r = result(c, id).get0();
        BOOST_REQUIRE_EQUAL(LDAP_RES_BIND, ldap_msgtype(r.get()));
    });
    mylog.trace("bind_after_fail done");
}

SEASTAR_THREAD_TEST_CASE(search_after_fail) {
    mylog.trace("search_after_fail: wonky connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& wonky_conn) {
        int id;
        if(LDAP_SUCCESS == search(wonky_conn, base_dn, &id)) {
            try_get_result(wonky_conn, id);
        }
    });
    mylog.trace("search_after_fail: solid connection");
    with_ldap_connection(local_ldap_address, [] (ldap_connection& c) {
        int msgid;
        BOOST_REQUIRE_EQUAL(LDAP_SUCCESS, search(c, base_dn, &msgid));
        mylog.trace("search_after_fail: search returned msgid {}", msgid);
        const std::set<std::string>& expected = results_expected_from_search_base_dn;
        const auto actual = entries(expected.size(), c, msgid);
        BOOST_REQUIRE_EQUAL_COLLECTIONS(actual.cbegin(), actual.cend(), expected.cbegin(), expected.cend());
    });
    mylog.trace("search_after_fail done");
}

SEASTAR_THREAD_TEST_CASE(multiple_outstanding_operations_on_failing_connection) {
    mylog.trace("multiple_outstanding_operations_on_failing_connection");
    with_ldap_connection(local_fail_inject_address, [] (ldap_connection& c) {
        const auto id_bind = bind(c);
        mylog.trace("bind_with_custom_sockbuf_io bind returned {}", id_bind);

        int id_search_base;
        mylog.trace("multiple_outstanding_operations_on_failing_connection: invoking search base");
        const auto status_base = search(c, base_dn, &id_search_base);
        mylog.trace("multiple_outstanding_operations_on_failing_connection: search base returned status {}, msgid {}",
                    status_base, id_search_base);

        int id_search_jsmith;
        mylog.trace("multiple_outstanding_operations_on_failing_connection: invoking search jsmith");
        const auto status_jsmith = search(c, "uid=jsmith,ou=People,dc=example,dc=com", &id_search_jsmith);
        mylog.trace("multiple_outstanding_operations_on_failing_connection: search jsmith returned status {}, msgid {}",
                    status_jsmith, id_search_jsmith);

        if (status_base == LDAP_SUCCESS) {
            mylog.trace("multiple_outstanding_operations_on_failing_connection: taking base-search result");
            for (size_t i = 0; i < results_expected_from_search_base_dn.size(); ++i) {
                try_get_result(c, id_search_base);
            }
        }

        if (id_bind != -1) {
            mylog.trace("multiple_outstanding_operations_on_failing_connection: taking bind result");
            try_get_result(c, id_bind);
        }

        if (status_jsmith == LDAP_SUCCESS) {
            mylog.trace("multiple_outstanding_operations_on_failing_connection: taking jsmith-search result");
            try_get_result(c, id_search_jsmith);
        }
    });
    mylog.trace("multiple_outstanding_operations_on_failing_connection done");
}
