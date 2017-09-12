/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include <map>
#include <unordered_map>
#include <tuple>
#include <stdexcept>
#include <regex>
#include <algorithm>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <boost/range/adaptor/map.hpp>

#include <seastar/core/seastar.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

#include "compress.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "bytes.hh"
#include "stdx.hh"

namespace encryption {

static const std::set<sstring> keywords = { KEY_PROVIDER,
                SECRET_KEY_PROVIDER_FACTORY_CLASS, SECRET_KEY_FILE, SYSTEM_KEY_FILE,
                CIPHER_ALGORITHM, IV_LENGTH, SECRET_KEY_STRENGTH, HOST_NAME,
                TEMPLATE_NAME, KEY_NAMESPACE
};

bytes base64_decode(const sstring& s, size_t off, size_t len) {
    if (off >= s.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, s.size() - off);
    auto n = (len / 4) * 3;
    bytes b{bytes::initialized_later(), n};
    auto r = EVP_DecodeBlock(reinterpret_cast<uint8_t*>(b.data()),
                    reinterpret_cast<const uint8_t *>(s.data() + off),
                    int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not decode: " + s);
    }
    b.resize(r);
    return b;
}

sstring base64_encode(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    auto n = ((len + 2) / 3) * 4;
    sstring s{sstring::initialized_later(), n};
    auto r = EVP_EncodeBlock(reinterpret_cast<uint8_t *>(s.data()),
                    reinterpret_cast<const uint8_t*>(b.data() + off), int(len));
    if (r < 0) {
        throw std::invalid_argument("Could not encode");
    }
    s.resize(r);
    return s;
}

bytes calculate_md5(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    bytes res{bytes::initialized_later(), MD5_DIGEST_LENGTH};
    MD5(reinterpret_cast<const uint8_t*>(b.data() + off), len, reinterpret_cast<uint8_t *>(res.data()));
    return res;
}

bytes calculate_sha256(const bytes& b, size_t off, size_t len) {
    if (off >= b.size()) {
        throw std::out_of_range("Invalid offset");
    }
    len = std::min(len, b.size() - off);
    bytes res{bytes::initialized_later(), SHA256_DIGEST_LENGTH};
    SHA256(reinterpret_cast<const uint8_t*>(b.data() + off), len, reinterpret_cast<uint8_t *>(res.data()));
    return res;
}

future<temporary_buffer<char>> read_text_file_fully(const sstring& filename) {
    return open_file_dma(filename, open_flags::ro).then([](file f) {
        return f.size().then([f](size_t s) {
            return do_with(make_file_input_stream(f), [s](input_stream<char>& in) {
                return in.read_exactly(s).then([](temporary_buffer<char> buf) {
                    return make_ready_future<temporary_buffer<char>>(std::move(buf));
                }).finally([&in] {
                    return in.close();
                });
            });
        });
    });
}

future<> write_text_file_fully(const sstring& filename, temporary_buffer<char> buf) {
    return open_file_dma(filename, open_flags::wo|open_flags::create).then([buf = std::move(buf)](file f) mutable {
        return do_with(make_file_output_stream(f), [buf = std::move(buf)](output_stream<char>& out) mutable {
            auto p = buf.get();
            auto s = buf.size();
            return out.write(p, s).finally([&out, buf = std::move(buf)] {
                return out.close();
            });
        });
    });
}

future<> write_text_file_fully(const sstring& filename, const sstring& s) {
    return write_text_file_fully(filename, temporary_buffer<char>(s.data(), s.size()));
}

class encryption_context_impl : public encryption_context {
    std::vector<std::unordered_map<sstring, shared_ptr<key_provider>>> _per_thread_provider_cache;
public:
    encryption_context_impl()
        : _per_thread_provider_cache(smp::count)
    {}

    shared_ptr<key_provider> get_provider(const options&) const {
        // not implemented
        return {};
    }
    shared_ptr<key_provider> get_cached_provider(const sstring& id) const override {
        auto& cache = _per_thread_provider_cache[engine().cpu_id()];
        auto i = cache.find(id);
        if (i != cache.end()) {
            return i->second;
        }
        return {};
    }
    void cache_provider(const sstring& id, shared_ptr<key_provider> p) override {
        _per_thread_provider_cache[engine().cpu_id()][id] = std::move(p);
    }
};

}
