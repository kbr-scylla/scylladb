/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "core/future-util.hh"
#include "audit/audit.hh"
#include "db/config.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/batch_statement.hh"
#include "storage_helper.hh"

namespace audit {

logging::logger logger("audit");

sstring audit_info::category() const {
    switch (_category) {
        case statement_category::QUERY: return "QUERY";
        case statement_category::DML: return "DML";
        case statement_category::DDL: return "DDL";
        case statement_category::DCL: return "DCL";
        case statement_category::AUTH: return "AUTH";
        case statement_category::ADMIN: return "ADMIN";
    }
    return "";
}

audit::audit(const db::config& cfg)
    : _storage_helper_class_name("audit_cf_storage_helper")
{ }

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
    try {
        _storage_helper_ptr = create_object<storage_helper>(_storage_helper_class_name);
    } catch (no_such_class& e) {
        logger.error("Can't create audit storage helper {}: not supported", _storage_helper_class_name);
        throw;
    } catch (...) {
        throw;
    }
    return _storage_helper_ptr->start();;
}

future<> audit::stop() {
    return _storage_helper_ptr->stop();
}

future<> audit::shutdown() {
    return make_ready_future<>();
}

future<> audit::log(const audit_info* audit_info, service::query_state& query_state, const cql3::query_options& options, bool error) {
    const service::client_state& client_state = query_state.get_client_state();
    net::ipv4_address node_ip = utils::fb_utilities::get_broadcast_address().addr();
    db::consistency_level cl = options.get_consistency();
    thread_local static sstring no_username("undefined");
    const sstring& username = client_state.user() ? client_state.user()->name() : no_username;
    net::ipv4_address client_ip = client_state.get_client_address().addr();
    return _storage_helper_ptr->write(audit_info, node_ip, client_ip, cl, username, error);
}

future<> inspect(shared_ptr<cql3::cql_statement> statement, service::query_state& query_state, const cql3::query_options& options, bool error) {
    cql3::statements::batch_statement* batch = dynamic_cast<cql3::statements::batch_statement*>(statement.get());
    if (batch != nullptr) {
        return do_for_each(batch->statements().begin(), batch->statements().end(), [&query_state, &options, error] (auto&& m) {
            return inspect(m, query_state, options, error);
        });
    } else {
        auto audit_info = statement->get_audit_info();
        if (bool(audit_info) && audit::local_audit_instance().should_log(audit_info)) {
            return audit::local_audit_instance().log(audit_info, query_state, options, error);
        }
    }
    return make_ready_future<>();
}

}
