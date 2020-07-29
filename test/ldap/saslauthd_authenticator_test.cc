/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <cstdlib>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "auth/saslauthd_authenticator.hh"
#include "db/config.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/exception_utils.hh"

const auto sockpath = std::getenv("SASLAUTHD_MUX_PATH");

using exceptions::authentication_exception;
using exception_predicate::message_contains;

SEASTAR_THREAD_TEST_CASE(simple_password_checking) {
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"jdoe", "xxxxxxxx", "", ""}).get0());
    BOOST_REQUIRE(auth::authenticate_with_saslauthd(sockpath, {"jdoe", "pa55w0rd", "", ""}).get0());
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"", "", "", ""}).get0());
    BOOST_REQUIRE(!auth::authenticate_with_saslauthd(sockpath, {"", "", ".", "."}).get0());
    BOOST_REQUIRE_EXCEPTION(
            auth::authenticate_with_saslauthd("/a/nonexistent/path", {"jdoe", "pa55w0rd", "", ""}).get0(),
            authentication_exception, message_contains("socket connection error"));
}

namespace {

shared_ptr<db::config> make_config() {
    auto p = make_shared<db::config>();
    p->authenticator("com.scylladb.auth.SaslauthdAuthenticator");
    p->saslauthd_socket_path(sockpath);
    return p;
}

const auth::authenticator& authenticator(cql_test_env& env) {
    return env.local_auth_service().underlying_authenticator();
}

/// Creates a cql_test_env with saslauthd_authenticator in a Seastar thread, then invokes func with the env's
/// authenticator.
future<> do_with_authenticator_thread(std::function<void(const auth::authenticator&)> func) {
    return do_with_cql_env_thread([func = std::move(func)] (cql_test_env& env) {
        return func(authenticator(env));
    }, make_config());
}

} // anonymous namespace

SEASTAR_TEST_CASE(require_authentication) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        BOOST_REQUIRE(authr.require_authentication());
    });
}

SEASTAR_TEST_CASE(authenticate) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        const auto user = auth::authenticator::USERNAME_KEY, pwd = auth::authenticator::PASSWORD_KEY;
        BOOST_REQUIRE_EQUAL(authr.authenticate({{user, "jdoe"}, {pwd, "pa55w0rd"}}).get0().name, "jdoe");
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{user, "jdoe"}, {pwd, ""}}).get0(),
                authentication_exception, message_contains("Incorrect credentials"));
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{user, "jdoe"}}).get0(),
                authentication_exception, message_contains("password' is missing"));
        BOOST_REQUIRE_EXCEPTION(
                authr.authenticate({{pwd, "pwd"}}).get0(),
                authentication_exception, message_contains("username' is missing"));
    });
}

SEASTAR_TEST_CASE(create) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        BOOST_REQUIRE_EXCEPTION(
                authr.create("new-role", {{"password"}}).get(),
                authentication_exception, message_contains("Cannot create"));
    });
}

SEASTAR_TEST_CASE(alter) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        BOOST_REQUIRE_EXCEPTION(
                authr.alter("jdoe", {{"password"}}).get(),
                authentication_exception, message_contains("Cannot modify"));
    });
}

SEASTAR_TEST_CASE(drop) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        BOOST_REQUIRE_EXCEPTION(authr.drop("jdoe").get(), authentication_exception, message_contains("Cannot delete"));
    });
}

SEASTAR_TEST_CASE(sasl_challenge) {
    return do_with_authenticator_thread([] (const auth::authenticator& authr) {
        constexpr char creds[] = "\0jdoe\0pa55w0rd";
        const auto ch = authr.new_sasl_challenge();
        BOOST_REQUIRE(ch->evaluate_response(bytes(creds, creds + 14)).empty());
        BOOST_REQUIRE_EQUAL("jdoe", ch->get_authenticated_user().get0().name);
        BOOST_REQUIRE(ch->evaluate_response(bytes(creds, creds + 13)).empty());
        BOOST_REQUIRE_EXCEPTION(
                ch->get_authenticated_user().get(),
                authentication_exception, message_contains("Incorrect credentials"));
    });
}
