/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <map>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace cdc {

enum class delta_mode {
    off,
    keys,
    full,
};

class options final {
    bool _enabled = false;
    bool _preimage = false;
    bool _postimage = false;
    delta_mode _delta_mode = delta_mode::full;
    int _ttl = 86400; // 24h in seconds
public:
    options() = default;
    options(const std::map<sstring, sstring>& map);

    std::map<sstring, sstring> to_map() const;
    sstring to_sstring() const;

    bool enabled() const { return _enabled; }
    bool preimage() const { return _preimage; }
    bool postimage() const { return _postimage; }
    delta_mode get_delta_mode() const { return _delta_mode; }
    int ttl() const { return _ttl; }

    void enabled(bool b) { _enabled = b; }
    void preimage(bool b) { _preimage = b; }
    void postimage(bool b) { _postimage = b; }
    void ttl(int v) { _ttl = v; }

    bool operator==(const options& o) const;
    bool operator!=(const options& o) const;
};

} // namespace cdc
