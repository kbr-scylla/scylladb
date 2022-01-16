/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <map>
#include <chrono>
#include <seastar/core/sstring.hh>

enum class tombstone_gc_mode : uint8_t { timeout, disabled, immediate, repair };

class tombstone_gc_options {
private:
    tombstone_gc_mode _mode = tombstone_gc_mode::timeout;
    std::chrono::seconds _propagation_delay_in_seconds = std::chrono::seconds(3600);
public:
    tombstone_gc_options() = default;
    const tombstone_gc_mode& mode() const { return _mode; }
    explicit tombstone_gc_options(const std::map<seastar::sstring, seastar::sstring>& map);
    const std::chrono::seconds& propagation_delay_in_seconds() const {
        return _propagation_delay_in_seconds;
    }
    std::map<seastar::sstring, seastar::sstring> to_map() const;
    seastar::sstring to_sstring() const;
    bool operator==(const tombstone_gc_options& other) const;
    bool operator!=(const tombstone_gc_options& other) const;
};

std::ostream& operator<<(std::ostream& os, const tombstone_gc_mode& m);
