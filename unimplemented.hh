/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <iosfwd>
#include "core/print.hh"
#include "core/sstring.hh"
#include "core/enum.hh"

namespace unimplemented {

enum class cause {
    API,
    INDEXES,
    LWT,
    PAGING,
    AUTH,
    PERMISSIONS,
    TRIGGERS,
    COUNTERS,
    METRICS,
    MIGRATIONS,
    GOSSIP,
    TOKEN_RESTRICTION,
    LEGACY_COMPOSITE_KEYS,
    COLLECTION_RANGE_TOMBSTONES,
    RANGE_DELETES,
    THRIFT,
    VALIDATION,
    REVERSED,
    COMPRESSION,
    NONATOMIC,
    CONSISTENCY,
    HINT,
    SUPER,
    WRAP_AROUND, // Support for handling wrap around ranges in queries on database level and below
    STORAGE_SERVICE,
    SCHEMA_CHANGE,
    MIXED_CF,
    VIEWS,
};

void fail(cause what) __attribute__((noreturn));
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : seastar::enum_hash<unimplemented::cause> {};

}
