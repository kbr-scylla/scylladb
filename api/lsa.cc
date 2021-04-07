/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "api/api-doc/lsa.json.hh"
#include "api/lsa.hh"
#include "api/api.hh"

#include <seastar/http/exception.hh>
#include "utils/logalloc.hh"
#include "log.hh"
#include "database.hh"

namespace api {

static logging::logger alogger("lsa-api");

void set_lsa(http_context& ctx, routes& r) {
    httpd::lsa_json::lsa_compact.set(r, [&ctx](std::unique_ptr<request> req) {
        alogger.info("Triggering compaction");
        return ctx.db.invoke_on_all([] (database&) {
            logalloc::shard_tracker().reclaim(std::numeric_limits<size_t>::max());
        }).then([] {
            return json::json_return_type(json::json_void());
        });
    });
}

}
