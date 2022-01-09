/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/net/ipv4_address.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <iosfwd>
#include <optional>
#include <functional>

#include "bytes.hh"
#include "seastarx.hh"

namespace gms {

class inet_address {
private:
    net::inet_address _addr;
public:
    inet_address() = default;
    inet_address(int32_t ip) noexcept
        : inet_address(uint32_t(ip)) {
    }
    explicit inet_address(uint32_t ip) noexcept
        : _addr(net::ipv4_address(ip)) {
    }
    inet_address(const net::inet_address& addr) noexcept : _addr(addr) {}
    inet_address(const socket_address& sa) noexcept
        : inet_address(sa.addr())
    {}
    const net::inet_address& addr() const noexcept {
        return _addr;
    }

    inet_address(const inet_address&) = default;

    operator const seastar::net::inet_address&() const noexcept {
        return _addr;
    }

    // throws std::invalid_argument if sstring is invalid
    inet_address(const sstring& addr) {
        // FIXME: We need a real DNS resolver
        if (addr == "localhost") {
            _addr = net::ipv4_address("127.0.0.1");
        } else {
            _addr = net::inet_address(addr);
        }
    }
    bytes_view bytes() const noexcept {
        return bytes_view(reinterpret_cast<const int8_t*>(_addr.data()), _addr.size());
    }
    // TODO remove
    uint32_t raw_addr() const {
        return addr().as_ipv4_address().ip;
    }
    sstring to_sstring() const;
    friend inline bool operator==(const inet_address& x, const inet_address& y) noexcept {
        return x._addr == y._addr;
    }
    friend inline bool operator!=(const inet_address& x, const inet_address& y) noexcept {
        using namespace std::rel_ops;
        return x._addr != y._addr;
    }
    friend inline bool operator<(const inet_address& x, const inet_address& y) noexcept {
        return x.bytes() < y.bytes();
    }
    friend struct std::hash<inet_address>;

    using opt_family = std::optional<net::inet_address::family>;

    static future<inet_address> lookup(sstring, opt_family family = {}, opt_family preferred = {});
};

std::ostream& operator<<(std::ostream& os, const inet_address& x);

}

namespace std {
template<>
struct hash<gms::inet_address> {
    size_t operator()(gms::inet_address a) const noexcept { return std::hash<net::inet_address>()(a._addr); }
};
}
