/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <unordered_set>
#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/tmpdir.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/make_random_string.hh"

#include "lister.hh"

class expected_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "expected_exception";
    }
};

SEASTAR_TEST_CASE(test_empty_lister) {
    auto tmp = tmpdir();
    size_t count = 0;
    co_await lister::scan_dir(tmp.path(), { directory_entry_type::directory, directory_entry_type::regular }, [&count] (fs::path dir, directory_entry de) {
        ++count;
        return make_ready_future<>();
    });
    BOOST_REQUIRE(!count);
}

static future<> touch_file(fs::path filename, open_flags flags = open_flags::wo | open_flags::create, file_permissions create_permissions = file_permissions::default_file_permissions) {
    file_open_options opts;
    opts.create_permissions = create_permissions;
    auto f = co_await open_file_dma(filename.native(), flags, opts);
    co_await f.close();
}

static future<size_t> generate_random_content(tmpdir& tmp, std::unordered_set<std::string>& file_names, std::unordered_set<std::string>& dir_names, size_t max_count = 1000) {
    size_t count = tests::random::get_int<size_t>(0, max_count);
    for (size_t i = 0; i < count; i++) {
        auto name = tests::random::get_sstring(tests::random::get_int(1, 8));
        if (tests::random::get_bool()) {
            if (!file_names.contains(name)) {
                dir_names.insert(name);
            }
        } else {
            if (!dir_names.contains(name)) {
                file_names.insert(name);
            }
        }
    }

    for (const auto& name : file_names) {
        co_await touch_file(tmp.path() / name);
    }
    for (const auto& name : dir_names) {
        co_await touch_directory((tmp.path() / name).native());
    }

    co_return file_names.size() + dir_names.size();
}

SEASTAR_TEST_CASE(test_lister_abort) {
    auto tmp = tmpdir();
    std::unordered_set<std::string> file_names;
    std::unordered_set<std::string> dir_names;

    auto count = co_await generate_random_content(tmp, file_names, dir_names, tests::random::get_int(100, 1000));
    BOOST_TEST_MESSAGE(fmt::format("Generated {} dir entries", count));

    size_t initial = tests::random::get_int<size_t>(1, count);
    BOOST_TEST_MESSAGE(fmt::format("Aborting lister after {} dir entries", initial));

    // test that throwing an exception from the walker
    // aborts the lister and that the exception is propagated
    // to the scan_dir resulting future.
    size_t walked = 0;
    auto f = lister::scan_dir(tmp.path(), { directory_entry_type::directory, directory_entry_type::regular }, [&walked, initial] (fs::path dir, directory_entry de) {
        if (++walked == initial) {
            throw expected_exception();
        }
        return make_ready_future<>();
    });
    BOOST_REQUIRE_THROW(co_await std::move(f), expected_exception);
    BOOST_REQUIRE_EQUAL(walked, initial);

    // similar to the above, just return an exceptional future
    // rather than throwing the exception.
    walked = 0;
    f = lister::scan_dir(tmp.path(), { directory_entry_type::directory, directory_entry_type::regular }, [&walked, initial] (fs::path dir, directory_entry de) {
        if (++walked == initial) {
            return make_exception_future<>(expected_exception());
        }
        return make_ready_future<>();
    });
    BOOST_REQUIRE_THROW(co_await std::move(f), expected_exception);
    BOOST_REQUIRE_EQUAL(walked, initial);
}
