/*
 * Copyright (C) 2016 ScyllaDB
 */



#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>
#include <random>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/align.hh>

#include <seastar/testing/test_case.hh>

#include "ent/encryption/encryption.hh"
#include "ent/encryption/symmetric_key.hh"

using namespace encryption;

static temporary_buffer<uint8_t> generate_random(size_t n, size_t align) {
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint8_t> dist('0', 'z');

    auto tmp = temporary_buffer<uint8_t>::aligned(align, align_up(n, align));
    std::generate(tmp.get_write(), tmp.get_write() + tmp.size(), std::bind(dist, std::ref(e1)));
    return tmp;
}

static void test_random_data(const sstring& desc, unsigned int bits) {
    auto buf = generate_random(128, 8);
    auto n = buf.size();

    // first, verify padded.
    {
        key_info info{desc, bits};
        auto k = ::make_shared<symmetric_key>(info);

        bytes b(bytes::initialized_later(), k->iv_len());
        k->generate_iv(b.data(), k->iv_len());

        temporary_buffer<uint8_t> tmp(n + k->block_size());
        k->encrypt(buf.get(), buf.size(), tmp.get_write(), tmp.size(), b.data());

        auto bytes = k->key();
        auto k2 = ::make_shared<symmetric_key>(info, bytes);

        temporary_buffer<uint8_t> tmp2(n + k->block_size());
        k2->decrypt(tmp.get(), tmp.size(), tmp2.get_write(), tmp2.size(), b.data());

        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp2.get(), tmp2.get() + n, buf.get(), buf.get() + n);
    }

    // unpadded
    {
        auto desc2 = desc;
        desc2.resize(desc.find_last_of('/'));
        key_info info{desc2, bits};
        auto k = ::make_shared<symmetric_key>(info);

        bytes b(bytes::initialized_later(), k->iv_len());
        k->generate_iv(b.data(), k->iv_len());

        temporary_buffer<uint8_t> tmp(n);
        k->encrypt_unpadded(buf.get(), buf.size(), tmp.get_write(), b.data());

        auto bytes = k->key();
        auto k2 = ::make_shared<symmetric_key>(info, bytes);

        temporary_buffer<uint8_t> tmp2(buf.size());
        k2->decrypt_unpadded(tmp.get(), tmp.size(), tmp2.get_write(), b.data());

        BOOST_REQUIRE_EQUAL_COLLECTIONS(tmp2.get(), tmp2.get() + n, buf.get(), buf.get() + n);
    }
}


SEASTAR_TEST_CASE(test_cipher_types) {
    static const std::unordered_map<sstring, std::vector<unsigned int>> ciphers = {
            { "AES/CBC/PKCS5Padding", { 128, 192, 256 } },
            { "AES/ECB/PKCS5Padding", { 128, 192, 256 } },
            { "DES/CBC/PKCS5Padding", { 56 } },
            { "DESede/CBC/PKCS5Padding", { 112, 168 } },
            { "Blowfish/CBC/PKCS5Padding", { 32, 64, 448 } },
            { "RC2/CBC/PKCS5Padding", { 40, 41, 64, 67, 120, 128 } },
    };

    for (auto & p : ciphers) {
        for (auto s : p.second) {
            test_random_data(p.first, s);
        }
    }
    return make_ready_future<>();
}
