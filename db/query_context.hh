/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once

#include <memory>
#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>
#include "cql3/query_processor.hh"

class database;

namespace service {
class storage_proxy;
}


namespace db {
struct query_context {
    distributed<database>& _db;
    distributed<cql3::query_processor>& _qp;
    query_context(distributed<database>& db, distributed<cql3::query_processor>& qp) : _db(db), _qp(qp) {}

    template <typename... Args>
    future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring req, Args&&... args) {
        return this->_qp.local().execute_internal(req, { data_value(std::forward<Args>(args))... });
    }
    database& db() {
        return _db.local();
    }

    service::storage_proxy& proxy() {
        return _qp.local().proxy();
    }

    cql3::query_processor& qp() {
        return _qp.local();
    }
};

// This does not have to be thread local, because all cores will share the same context.
extern std::unique_ptr<query_context> qctx;

// Sometimes we are not concerned about system tables at all - for instance, when we are testing. In those cases, just pretend
// we executed the query, and return an empty result
template <typename... Args>
static future<::shared_ptr<cql3::untyped_result_set>> execute_cql(sstring text, Args&&... args) {
    assert(qctx);
    return qctx->execute_cql(text, std::forward<Args>(args)...);
}

}
