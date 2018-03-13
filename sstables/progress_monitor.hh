/*
 * Copyright (C) 2017 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once


#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_ptr_incomplete.hh>

namespace sstables {
class write_monitor {
public:
    virtual ~write_monitor() { }
    virtual void on_write_completed() = 0;
    virtual void on_flush_completed() = 0;
};

struct noop_write_monitor final : public write_monitor {
    virtual void on_write_completed() override { }
    virtual void on_flush_completed() override { }
};

seastar::shared_ptr<write_monitor> default_write_monitor();
}
