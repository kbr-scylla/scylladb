/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "api/api-doc/lsa.json.hh"
#include "api/lsa.hh"
#include "api/api.hh"

#include <seastar/http/exception.hh>
#include "utils/logalloc.hh"
#include "log.hh"
#include "replica/database.hh"

namespace api {

static logging::logger alogger("lsa-api");

void set_lsa(http_context& ctx, routes& r) {
    httpd::lsa_json::lsa_compact.set(r, [&ctx](std::unique_ptr<request> req) {
        alogger.info("Triggering compaction");
        return ctx.db.invoke_on_all([] (replica::database&) {
            logalloc::shard_tracker().reclaim(std::numeric_limits<size_t>::max());
        }).then([] {
            return json::json_return_type(json::json_void());
        });
    });
}

}
