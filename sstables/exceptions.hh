/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/print.hh>

#include "seastarx.hh"

namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring msg, sstring filename)
        : malformed_sstable_exception{format("{} in sstable {}", msg, filename)}
    {}
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(format("Buffer improperly sized to hold requested data. Got: {:d}. Expected: {:d}", size, expected))
    {}
};

class compaction_stop_exception : public std::exception {
    sstring _msg;
    bool _retry;
public:
    compaction_stop_exception(sstring ks, sstring cf, sstring reason, bool retry = false) :
        _msg(format("Compaction for {}/{} was stopped due to: {}", ks, cf, reason)), _retry(retry) {}
    bool retry() const {
        return _retry;
    }
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

}
