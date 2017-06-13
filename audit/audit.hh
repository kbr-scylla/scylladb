/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <seastar/core/sharded.hh>
#include "stdx.hh"

namespace db {

class config;

}

namespace audit {

class audit final : public seastar::async_sharded_service<audit> {
public:
    static seastar::sharded<audit>& audit_instance() {
        // FIXME: leaked intentionally to avoid shutdown problems, see #293
        static seastar::sharded<audit>* audit_inst = new seastar::sharded<audit>();

        return *audit_inst;
    }

    static audit& local_audit_instance() {
        return audit_instance().local();
    }
    static future<> create_audit(const db::config& cfg);
    static future<> start_audit();
    static future<> stop_audit();
    audit(const db::config& cfg);
    future<> start();
    future<> stop();
    future<> shutdown();
};


}
