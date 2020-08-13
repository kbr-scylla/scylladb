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

enum class delta_mode : uint8_t {
    off,
    keys,
    full,
};

/**
 * (for now only pre-) image collection mode.
 * Stating how much info to record.
 * off == none
 * on == changed columns
 * full == all (changed and unmodified columns)
 */
enum class image_mode : uint8_t {
    off, 
    on,
    full,
};

std::ostream& operator<<(std::ostream& os, delta_mode);
std::ostream& operator<<(std::ostream& os, image_mode);

class options final {
    bool _enabled = false;
    image_mode _preimage = image_mode::off;
    bool _postimage = false;
    delta_mode _delta_mode = delta_mode::full;
    int _ttl = 86400; // 24h in seconds
public:
    options() = default;
    options(const std::map<sstring, sstring>& map);

    std::map<sstring, sstring> to_map() const;
    sstring to_sstring() const;

    bool enabled() const { return _enabled; }
    bool preimage() const { return _preimage != image_mode::off; }
    bool full_preimage() const { return _preimage == image_mode::full; }
    bool postimage() const { return _postimage; }
    delta_mode get_delta_mode() const { return _delta_mode; }
    int ttl() const { return _ttl; }

    void enabled(bool b) { _enabled = b; }
    void preimage(bool b) { preimage(b ? image_mode::on : image_mode::off); }
    void preimage(image_mode m) { _preimage = m; }
    void postimage(bool b) { _postimage = b; }
    void ttl(int v) { _ttl = v; }

    bool operator==(const options& o) const;
    bool operator!=(const options& o) const;
};

} // namespace cdc
