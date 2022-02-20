/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <vector>
#include <stdexcept>
#include "utils/exception_container.hh"
#include "utils/result.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;

class foo_exception : public std::exception {
public:
    const char* what() const noexcept override {
        return "foo";
    }
};

class bar_exception : public std::exception {
public:
    const char* what() const noexcept override {
        return "bar";
    }
};

using exc_container = utils::exception_container<foo_exception, bar_exception>;

template<typename T = void>
using result = bo::result<T, exc_container,utils::exception_container_throw_policy>;

SEASTAR_TEST_CASE(test_exception_container_throw_policy) {
    result<> r_ok = bo::success();
    BOOST_REQUIRE_NO_THROW(r_ok.value());
    BOOST_REQUIRE_THROW(r_ok.error(), bo::bad_result_access);

    result<> r_err_foo = bo::failure(foo_exception());
    BOOST_REQUIRE_NO_THROW(r_err_foo.error());
    BOOST_REQUIRE_THROW(r_err_foo.value(), foo_exception);

    return make_ready_future<>();
}

SEASTAR_THREAD_TEST_CASE(test_result_into_future) {
    // T == void

    result<> r_ok = bo::success();
    auto f_ok = utils::result_into_future(std::move(r_ok));
    BOOST_REQUIRE_NO_THROW(f_ok.get());

    result<> r_err_foo = bo::failure(foo_exception());
    auto f_err_foo = utils::result_into_future(std::move(r_err_foo));
    BOOST_REQUIRE_THROW(f_err_foo.get(), foo_exception);

    // T != void

    result<int> r_ok_int = bo::success();
    auto f_ok_int = utils::result_into_future(std::move(r_ok_int));
    BOOST_REQUIRE_NO_THROW(f_ok_int.get());

    result<int> r_err_foo_int = bo::failure(foo_exception());
    auto f_err_foo_int = utils::result_into_future(std::move(r_err_foo_int));
    BOOST_REQUIRE_THROW(f_err_foo_int.get(), foo_exception);
}

SEASTAR_THREAD_TEST_CASE(test_then_ok_result) {
    auto f_void = utils::then_ok_result<result<>>(make_ready_future<>());
    BOOST_REQUIRE_NO_THROW(f_void.get().value());

    auto f_int = utils::then_ok_result<result<int>>(make_ready_future<int>(123));
    BOOST_REQUIRE_EQUAL(f_int.get().value(), 123);
}

SEASTAR_THREAD_TEST_CASE(test_result_wrap) {
    int run_count = 0;

    // T == void
    auto fun_void = utils::result_wrap([&run_count] {
        ++run_count;
        return result<>(bo::success());
    });

    BOOST_REQUIRE_NO_THROW(fun_void(result<>(bo::success())).get().value());
    BOOST_REQUIRE_EQUAL(run_count, 1);

    BOOST_REQUIRE_THROW(fun_void(result<>(bo::failure(foo_exception()))).get().value(), foo_exception);
    BOOST_REQUIRE_EQUAL(run_count, 1);

    // T != void
    auto fun_int = utils::result_wrap([&run_count] (int i) {
        ++run_count;
        return result<int>(bo::success(i));
    });

    BOOST_REQUIRE_EQUAL(fun_int(result<int>(bo::success(123))).get().value(), 123);
    BOOST_REQUIRE_EQUAL(run_count, 2);

    BOOST_REQUIRE_THROW(fun_int(result<int>(bo::failure(foo_exception()))).get().value(), foo_exception);
    BOOST_REQUIRE_EQUAL(run_count, 2);
}

SEASTAR_THREAD_TEST_CASE(test_result_parallel_for_each) {
    auto reduce = [] (auto... params) {
        std::vector<result<>> v;
        (v.push_back(std::move(params)), ...);
        utils::result_parallel_for_each<result<>>(std::move(v), [] (result<>& r) {
            return make_ready_future<result<>>(std::move(r));
        }).get().value(); // <- trying to access the value throws in case of error
    };

    auto foo_exc = [] () { return result<>(bo::failure(foo_exception())); };
    auto bar_exc = [] () { return result<>(bo::failure(bar_exception())); };

    BOOST_REQUIRE_NO_THROW(reduce(bo::success(), bo::success()));
    BOOST_REQUIRE_THROW(reduce(foo_exc(), bo::success()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(bo::success(), foo_exc()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(foo_exc(), bar_exc()), foo_exception);
    BOOST_REQUIRE_THROW(reduce(bar_exc(), foo_exc()), bar_exception);
}
