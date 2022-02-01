/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */


#include "UUID.hh"
#include <seastar/net/byteorder.hh>
#include <random>
#include <boost/iterator/function_input_iterator.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <seastar/core/sstring.hh>
#include "utils/serialization.hh"
#include "marshal_exception.hh"

namespace utils {

UUID
make_random_uuid() {
    static thread_local std::mt19937_64 engine(std::random_device().operator()());
    static thread_local std::uniform_int_distribution<uint64_t> dist;
    uint64_t msb, lsb;
    msb = dist(engine);
    lsb = dist(engine);
    msb &= ~uint64_t(0x0f << 12);
    msb |= 0x4 << 12; // version 4
    lsb &= ~(uint64_t(0x3) << 62);
    lsb |= uint64_t(0x2) << 62; // IETF variant
    return UUID(msb, lsb);
}

std::ostream& operator<<(std::ostream& out, const UUID& uuid) {
    return out << uuid.to_sstring();
}

UUID::UUID(sstring_view uuid) {
    sstring uuid_string(uuid.begin(), uuid.end());
    boost::erase_all(uuid_string, "-");
    auto size = uuid_string.size() / 2;
    if (size != 16) {
        throw marshal_exception(format("UUID string size mismatch: '{}'", uuid));
    }
    sstring most = sstring(uuid_string.begin(), uuid_string.begin() + size);
    sstring least = sstring(uuid_string.begin() + size, uuid_string.end());
    int base = 16;
    this->most_sig_bits = std::stoull(most, nullptr, base);
    this->least_sig_bits = std::stoull(least, nullptr, base);
}

}
