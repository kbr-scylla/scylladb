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

#include <memory>

namespace db {

class config;

}

namespace audit {

enum class statement_category {
    QUERY, DML, DDL, DCL, AUTH, ADMIN
};

class audit_info final {
    statement_category _category;
    sstring _keyspace;
    sstring _table;
public:
    audit_info(statement_category cat, sstring keyspace, sstring table)
        : _category(cat)
        , _keyspace(std::move(keyspace))
        , _table(std::move(table))
    { }
};

using audit_info_ptr = std::unique_ptr<audit_info>;

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
    static audit_info_ptr create_audit_info(statement_category cat, const sstring& keyspace, const sstring& table);
    audit(const db::config& cfg);
    future<> start();
    future<> stop();
    future<> shutdown();
};


}
