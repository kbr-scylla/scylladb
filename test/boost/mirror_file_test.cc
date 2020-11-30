/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <fmt/format.h>

#include <seastar/core/seastar.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "seastarx.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/random_utils.hh"
#include "in-memory-file-impl.hh"
#include "mirror-file-impl.hh"

namespace fs = std::filesystem;

static const sstring test_file_name = "mirrored_file.txt";

// called in a seastar thread
static file prepare_test_file(const tmpdir& tmp, const sstring& name = test_file_name, ssize_t size = -1) {
    if (size < 0) {
        size = tests::random::get_int(128 * 1024);
    }
    auto fpath = tmp.path() / name.c_str();
    auto f = open_file_dma(fpath.native(), open_flags::create | open_flags::rw).get0();
    auto aligned_size = align_up(size_t(size), f.disk_write_dma_alignment());
    // BOOST_TEST_MESSAGE(fmt::format("prepare_test_file: path={} size={} aligned_size={}", fpath.native(), size, aligned_size));
    auto buf = allocate_aligned_buffer<char>(aligned_size, f.memory_dma_alignment());
    auto wbuf = buf.get();
    auto bytes = tests::random::get_bytes(size);
    std::copy_n(bytes.data(), size, wbuf);
    f.dma_write(0, wbuf, aligned_size).get();
    f.truncate(size).get();
    f.close().get();
    return open_file_dma(fpath.native(), open_flags::ro).get0();
}

SEASTAR_THREAD_TEST_CASE(test_mirror_file_create_delete) {
    init_in_memory_file_store(smp::count);
    auto img = defer([] { deinit_in_memory_file_store().get(); });
    auto tmp = tmpdir();
    auto name = test_file_name;
    auto primary = prepare_test_file(tmp, name);
    auto fpath = tmp.path() / name.c_str();
    auto mirrored = make_in_memory_mirror_file(primary, fpath.native()).get0();
    mirrored.close().get();
    BOOST_REQUIRE(file_exists(fpath.native()).get0());
    remove_mirrored_file(fpath.native()).get();
    BOOST_REQUIRE(!file_exists(fpath.native()).get0());
    BOOST_REQUIRE_THROW(remove_mirrored_file(fpath.native()).get(), std::system_error);
}

SEASTAR_THREAD_TEST_CASE(test_mirror_file_rename) {
    init_in_memory_file_store(smp::count);
    auto img = defer([] { deinit_in_memory_file_store().get(); });
    auto tmp = tmpdir();
    auto old_name = test_file_name;
    auto primary = prepare_test_file(tmp, old_name);
    auto old_path = tmp.path() / old_name.c_str();
    auto mirrored = make_in_memory_mirror_file(primary, old_path.native()).get0();
    mirrored.close().get();
    BOOST_REQUIRE(file_exists(old_path.native()).get0());
    auto new_name = sstring("renamed_") + old_name;
    auto new_path = tmp.path() / new_name.c_str();
    rename_mirrored_file(old_path.native(), new_path.native()).get();
    BOOST_REQUIRE(!file_exists(old_path.native()).get0());
    BOOST_REQUIRE(file_exists(new_path.native()).get0());
    remove_mirrored_file(new_path.native()).get();
    BOOST_REQUIRE_THROW(remove_mirrored_file(old_path.native()).get(), std::system_error);
    BOOST_REQUIRE_THROW(remove_mirrored_file(new_path.native()).get(), std::system_error);
}

SEASTAR_THREAD_TEST_CASE(test_mirror_file_link) {
    init_in_memory_file_store(smp::count);
    auto img = defer([] { deinit_in_memory_file_store().get(); });
    auto tmp = tmpdir();
    auto old_name = test_file_name;
    auto primary = prepare_test_file(tmp, old_name);
    auto old_path = tmp.path() / old_name.c_str();
    auto mirrored = make_in_memory_mirror_file(primary, old_path.native()).get0();
    mirrored.close().get();
    BOOST_REQUIRE(file_exists(old_path.native()).get0());
    auto new_name = sstring("renamed_") + old_name;
    auto new_path = tmp.path() / new_name.c_str();
    link_mirrored_file(old_path.native(), new_path.native()).get();
    BOOST_REQUIRE(file_exists(old_path.native()).get0());
    BOOST_REQUIRE(file_exists(new_path.native()).get0());
    remove_mirrored_file(old_path.native()).get();
    BOOST_REQUIRE(file_exists(new_path.native()).get0());
    remove_mirrored_file(new_path.native()).get();
    BOOST_REQUIRE_THROW(remove_mirrored_file(old_path.native()).get(), std::system_error);
    BOOST_REQUIRE_THROW(remove_mirrored_file(new_path.native()).get(), std::system_error);
}
