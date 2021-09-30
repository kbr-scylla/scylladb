/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include <stdexcept>
#include <regex>
#include <algorithm>

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <seastar/core/align.hh>
#include <seastar/core/print.hh>

#include "symmetric_key.hh"
#include "utils/hash.hh"

static const bool inited = [] { OpenSSL_add_all_ciphers(); return true; }();

std::ostream& encryption::operator<<(std::ostream& os, const key_info& info) {
    return os << info.alg << ":" << info.len;
}

bool encryption::key_info::compatible(const key_info& rhs) const {
    sstring malg, halg;
    std::tie(malg, std::ignore, std::ignore) = parse_key_spec(alg);
    std::tie(halg, std::ignore, std::ignore) = parse_key_spec(rhs.alg);
    if (malg != halg) {
        return false;
    }
    // If lengths differ we need to actual create keys to
    // check what the true lengths are. Since openssl and
    // java designators count different for DES etc.
    if (len != rhs.len) {
        symmetric_key k1(*this);
        symmetric_key k2(rhs);
        if (k1.key().size() != k2.key().size()) {
            return false;
        }
    }
    return true;
}

std::tuple<sstring, sstring, sstring>
encryption::parse_key_spec(const sstring& alg) {
    static const std::regex alg_exp(R"foo(^(\w+)(?:\/(\w+))?(?:\/(\w+))?$)foo");

    std::cmatch m;
    if (!std::regex_match(alg.begin(), alg.end(), m, alg_exp)) {
        throw std::invalid_argument("Invalid algorithm string: " + alg);
    }

    auto type = m[1].str();
    auto mode = m[2].str();
    auto padd = m[3].str();

    std::transform(type.begin(), type.end(), type.begin(), ::tolower);
    std::transform(mode.begin(), mode.end(), mode.begin(), ::tolower);
    std::transform(padd.begin(), padd.end(), padd.begin(), ::tolower);

    static const std::string padding = "padding";
    if (padd.size() > padding.size() && std::equal(padding.rbegin(), padding.rend(), padd.rbegin())) {
        padd.resize(padd.size() - padding.size());
    }

    return std::make_tuple<sstring, sstring, sstring>(type, mode, padd);
}

encryption::symmetric_key::symmetric_key(const key_info& info, const bytes& key)
    : _ctxt(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free)
    , _info(info)
    , _key(key)
{
    if (!_ctxt) {
        throw std::bad_alloc();
    }

    sstring type, mode, padd;
    std::tie(type, mode, padd) = parse_key_spec(info.alg);

    // Note: we are using some types here that are explicitly marked as "unsupported - placeholder"
    // in gnutls.

    // openssl does not allow missing block mode. so default one.
    if (mode.empty()) {
        mode = "cbc";
    }
    // camel case vs. dash
    if (type == "desede") {
        type = "des-ede";
        // and 168-bits desede is ede3 in openssl...
        if (info.len > 16*8) {
            type = "des-ede3";
        }
    }
    // OpenSSL only supports one form of padding. We used to just allow
    // non-empty string -> pkcs5/pcks7. Better to verify
    // (note: pcks5 is sortof a misnomeanor here, as in the Sun world, it
    // sort of means "pkcs7 with automatic block size" - which is pretty
    // much how things are in the OpenSSL universe as well)
    if (padd == "no") {
        padd = "";
    }
    if (!padd.empty() && padd != "pkcs5" && padd != "pkcs" && padd != "pkcs7") {
        throw std::invalid_argument("non-supported padding option: " + padd);
    }

    auto str = sprint("%s-%d-%s", type, info.len, mode);
    auto cipher = EVP_get_cipherbyname(str.c_str());

    if (!cipher) {
        str = sprint("%s-%s", type, mode);
        cipher = EVP_get_cipherbyname(str.c_str());
    }
    if (!cipher) {
        str = sprint("%s-%d", type, info.len);
        cipher = EVP_get_cipherbyname(str.c_str());
    }
    if (!cipher) {
        str = type;
        cipher = EVP_get_cipherbyname(str.c_str());
    }
    if (!cipher) {
        throw std::invalid_argument("Invalid algorithm: " + info.alg);
    }

    size_t len = EVP_CIPHER_key_length(cipher);

    if ((_info.len/8) != len) {
        if (!EVP_CipherInit_ex(*this, cipher, nullptr, nullptr, nullptr, 0)) {
            throw std::runtime_error("Could not initialize cipher");
        }
        auto dlen = _info.len/8;
        // Openssl describes des-56 length as 64 (counts parity),
        // des-ede-112 as 128 etc...
        // do some special casing...
        if ((type == "des" || type == "des-ede" || type == "des-ede3") && (dlen & 7) != 0) {
            dlen = align_up(dlen, 8u);
        }
        // if we had to find a cipher without explicit key length (like rc2),
        // try to set the key length to the desired strength.
        if (!EVP_CIPHER_CTX_set_key_length(*this, dlen)) {
            throw std::invalid_argument(sprint("Invalid length %d for resolved type %s (wanted %d)", len*8, str, _info.len));
        }

        len = EVP_CIPHER_key_length(cipher);
    }


    if (_key.empty()) {
        _key.resize(len);
        if (!RAND_bytes(reinterpret_cast<uint8_t*>(_key.data()), _key.size())) {
            throw std::runtime_error("Could not generate key: " + info.alg);
        }
    }
    if (_key.size() < len) {
        throw std::invalid_argument(sprint("Invalid key data length %d for resolved type %s (%d)", _key.size()*8, str, len*8));
    }

    if (!EVP_CipherInit_ex(*this, cipher, nullptr,
                    reinterpret_cast<const uint8_t*>(_key.data()), nullptr,
                    0)) {
        throw std::runtime_error("Could not initialize cipher");
    }

    _iv_len = EVP_CIPHER_CTX_iv_length(*this);
    _block_size = EVP_CIPHER_CTX_block_size(*this);
    _padding = !padd.empty();

}

void encryption::symmetric_key::generate_iv_impl(uint8_t* dst, size_t s) const {
    if (s < _iv_len) {
        throw std::invalid_argument("Buffer underflow");
    }
    if (!RAND_bytes(dst, s)) {
        throw std::runtime_error("Could not generate initialization vector");
    }
}

void encryption::symmetric_key::transform_unpadded_impl(const uint8_t* input,
                size_t input_len, uint8_t* output, const uint8_t* iv, mode m) const {
    if (!EVP_CipherInit_ex(*this, nullptr, nullptr,
                    reinterpret_cast<const uint8_t*>(_key.data()), iv, int(m))) {
        throw std::runtime_error("Could not initialize cipher");
    }
    if (!EVP_CIPHER_CTX_set_padding(*this, 0)) {
        throw std::runtime_error("Could not disable padding");
    }

    if (input_len & (_block_size - 1)) {
        throw std::invalid_argument("Data must be aligned to 'blocksize'");
    }

    int outl = 0;
    auto res = m == mode::decrypt ?
                    EVP_DecryptUpdate(*this, output, &outl, input,
                                    int(input_len)) :
                    EVP_EncryptUpdate(*this, output, &outl, input,
                                    int(input_len));

    if (!res || outl != int(input_len)) {
        throw std::runtime_error("transformation failed");
    }
}

size_t encryption::symmetric_key::decrypt_impl(const uint8_t* input,
                size_t input_len, uint8_t* output, size_t output_len,
                const uint8_t* iv) const {
    if (!EVP_CipherInit_ex(*this, nullptr, nullptr,
                    reinterpret_cast<const uint8_t*>(_key.data()), iv, 0)) {
        throw std::runtime_error("Could not initialize cipher");
    }
    if (!EVP_CIPHER_CTX_set_padding(*this, int(_padding))) {
        throw std::runtime_error("Could not initialize padding");
    }

    // normal case, caller provides output enough to deal with any padding.
    // in padding case, max out size is input_len - 1.
    if (input_len <= output_len) {
        // one go.
        int outl = 0;
        int finl = 0;
        if (!EVP_DecryptUpdate(*this, output, &outl, input, int(input_len))) {
            throw std::runtime_error("decryption failed");
        }
        if (!EVP_DecryptFinal(*this, output + outl, &finl)) {
            throw std::runtime_error("decryption failed");
        }

        return outl + finl;
    }

    // meh. must provide block padding.
    constexpr size_t local_buf_size = 1024;

    unsigned char buf[local_buf_size + _block_size];
    size_t res = 0;
    while (input_len) {
        auto n = std::min(input_len, local_buf_size);
        int outl = 0;
        if (!EVP_DecryptUpdate(*this, buf, &outl, input, int(n))) {
            throw std::runtime_error("decryption failed");
        }
        if (n < local_buf_size) {
            // last block
            int finl = 0;
            if (!EVP_DecryptFinal(*this, buf + outl, &finl)) {
                throw std::runtime_error("decryption failed");
            }
            outl += finl;
        }
        if ((res + outl) > output_len) {
            throw std::invalid_argument("Output buffer too small");
        }
        output = std::copy(buf, buf + outl, output);
        res += outl;
        input_len -= n;
        input += n;
    }

    return res;
}

size_t encryption::symmetric_key::encrypted_size(size_t n) const {
    // encryption always adds padding. So if n is multiple of blocksize
    // the size is n + blocksize. But if its not, things are "better"...
    return _block_size + align_down<size_t>(n, _block_size);
}

size_t encryption::symmetric_key::encrypt_impl(const uint8_t* input,
                size_t input_len, uint8_t* output, size_t output_len,
                const uint8_t* iv) const {
    if (output_len < encrypted_size(input_len)) {
        throw std::invalid_argument("Insufficient buffer");
    }

    if (!EVP_CipherInit_ex(*this, nullptr, nullptr,
                    reinterpret_cast<const uint8_t*>(_key.data()), iv, 1)) {
        throw std::runtime_error("Could not initialize cipher");
    }
    if (!EVP_CIPHER_CTX_set_padding(*this, int(_padding))) {
        throw std::runtime_error("Could not initialize padding");
    }

    int outl = 0;
    int finl = 0;
    if (!EVP_EncryptUpdate(*this, output, &outl, input, int(input_len))) {
        throw std::runtime_error("encryption failed");
    }
    if (!EVP_EncryptFinal(*this, output + outl, &finl)) {
        throw std::runtime_error("encryption failed");
    }
    return outl + finl;
}

bool encryption::operator==(const key_info& k1, const key_info& k2) {
    return k1.alg == k2.alg && k1.len == k2.len;
}

bool encryption::operator!=(const key_info& k1, const key_info& k2) {
    return !(k1 == k2);
}

size_t encryption::key_info_hash::operator()(const key_info& e) const {
    return utils::tuple_hash()(std::tie(e.alg, e.len));
}
