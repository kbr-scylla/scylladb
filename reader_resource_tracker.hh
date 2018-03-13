/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

/*
 * Copyright (C) 2017 ScyllaDB
 */

#pragma once

#include <core/file.hh>
#include <core/semaphore.hh>

class reader_resource_tracker {
    seastar::semaphore* _sem = nullptr;
public:
    reader_resource_tracker() = default;
    explicit reader_resource_tracker(seastar::semaphore* sem)
        : _sem(sem) {
    }

    bool operator==(const reader_resource_tracker& other) const {
        return _sem == other._sem;
    }

    file track(file f) const;

    semaphore* get_semaphore() const {
        return _sem;
    }
};

inline reader_resource_tracker no_resource_tracking() {
    return reader_resource_tracker(nullptr);
}
