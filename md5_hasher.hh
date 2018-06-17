/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1

#include <cryptopp/md5.h>
#include "hashing.hh"
#include "bytes.hh"

class md5_hasher {
    CryptoPP::Weak::MD5 hash{};
public:
    static constexpr size_t size = CryptoPP::Weak::MD5::DIGESTSIZE;

    void update(const char* ptr, size_t length) {
        using namespace CryptoPP;
        static_assert(sizeof(char) == sizeof(byte), "Assuming lengths will be the same");
        hash.Update(reinterpret_cast<const byte*>(ptr), length * sizeof(byte));
    }

    bytes finalize() {
        bytes digest{bytes::initialized_later(), size};
        hash.Final(reinterpret_cast<unsigned char*>(digest.begin()));
        return digest;
    }

    std::array<uint8_t, size> finalize_array() {
        std::array<uint8_t, size> array;
        hash.Final(reinterpret_cast<unsigned char*>(array.data()));
        return array;
    }
};
