/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "partition_version.hh"

// Double-ended chained list of partition_version objects
// utilizing partition_version's intrinsic anchorless_list_base_hook.
class partition_version_list {
    partition_version_ref _head;
    partition_version_ref _tail; // nullptr means _tail == _head.
public:
    // Appends v to the tail of this deque.
    // The version must not be already referenced.
    void push_back(partition_version& v) noexcept {
        if (!_tail) {
            if (_head) {
                v.insert_before(*_head);
                _tail = std::move(_head);
            }
            _head = partition_version_ref(v);
        } else {
            v.insert_after(*_tail);
            _tail = partition_version_ref(v);
        }
    }

    // Returns a reference to the first version in this deque.
    // Call only if !empty().
    partition_version& front() noexcept {
        return *_head;
    }

    // Returns true iff contains any versions.
    bool empty() const noexcept {
        return !_head;
    }

    // Detaches the first version from the list.
    // Assumes !empty().
    void pop_front() noexcept {
        if (_tail && _head->next() == &*_tail) {
            _tail = {};
        }
        partition_version* next = _head->next();
        _head->erase();
        _head.release();
        if (next) {
            _head = partition_version_ref(*next);
        }
    }

    // Appends other to the tail of this deque.
    // The other deque will be left empty.
    void splice(partition_version_list& other) noexcept {
        if (!other._head) {
            return;
        }
        if (!_head) {
            _head = std::move(other._head);
            _tail = std::move(other._tail);
        } else {
            (_tail ? _tail : _head)->splice(*other._head);
            if (other._tail) {
                _tail = std::move(other._tail);
                other._head = {};
            } else {
                _tail = std::move(other._head);
            }
        }
    }
};
