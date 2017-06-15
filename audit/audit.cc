/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "audit.hh"
#include "db/config.hh"

namespace audit {

audit::audit(const db::config& cfg) {
}

future<> audit::create_audit(const db::config& cfg) {
    if (cfg.audit() != "table") {
        return make_ready_future<>();
    }
    return audit_instance().start(cfg);
}

future<> audit::start_audit() {
    if (!audit_instance().local_is_initialized()) {
        return make_ready_future<>();
    }
    return audit_instance().invoke_on_all([] (audit& local_audit) {
        return local_audit.start();
    });
}

future<> audit::stop_audit() {
    if (!audit_instance().local_is_initialized()) {
        return make_ready_future<>();
    }
    return audit::audit::audit_instance().invoke_on_all([] (auto& local_audit) {
        return local_audit.shutdown();
    }).then([] {
        return audit::audit::audit_instance().stop();
    });
}

audit_info_ptr audit::create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table) {
    if (!audit_instance().local_is_initialized()) {
        return nullptr;
    }
    return std::make_unique<audit_info>(cat, keyspace, table);
}

audit_info_ptr audit::create_no_audit_info() {
    return audit_info_ptr();
}

future<> audit::start() {
    return make_ready_future<>();
}

future<> audit::stop() {
    return make_ready_future<>();
}

future<> audit::shutdown() {
    return make_ready_future<>();
}

}
