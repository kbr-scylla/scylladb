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
namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring msg, sstring filename)
        : malformed_sstable_exception{sprint("%s in sstable %s", msg, filename)}
    {}
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(sprint("Buffer improperly sized to hold requested data. Got: %ld. Expected: %ld", size, expected))
    {}
};

class compaction_stop_exception : public std::exception {
    sstring _msg;
public:
    compaction_stop_exception(sstring ks, sstring cf, sstring reason) :
        _msg(sprint("Compaction for %s/%s was stopped due to %s.", ks, cf, reason)) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

}
