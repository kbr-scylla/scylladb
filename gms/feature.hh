/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/shared_future.hh>

namespace gms {

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A feature should only be created once the gossiper is available.
 */
class feature final {
    sstring _name;
    bool _enabled = false;
    mutable shared_promise<> _pr;
    friend class gossiper;
public:
    explicit feature(sstring name, bool enabled = false);
    feature() = default;
    ~feature();
    feature(const feature& other) = delete;
    void enable();
    feature& operator=(feature&& other);
    const sstring& name() const {
        return _name;
    }
    explicit operator bool() const {
        return _enabled;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const feature& f) {
        return os << "{ gossip feature = " << f._name << " }";
    }
    future<> when_enabled() const { return _pr.get_shared_future(); }
};

} // namespace gms
