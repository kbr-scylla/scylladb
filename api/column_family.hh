/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "api.hh"
#include "api/api-doc/column_family.json.hh"
#include "database.hh"

namespace api {

void set_column_family(http_context& ctx, routes& r);

const utils::UUID& get_uuid(const sstring& name, const database& db);
future<> foreach_column_family(http_context& ctx, const sstring& name, std::function<void(column_family&)> f);


template<class Mapper, class I, class Reducer>
future<I> map_reduce_cf_raw(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([mapper, uuid](database& db) {
        return mapper(db.find_column_family(uuid));
    }, init, reducer);
}


template<class Mapper, class I, class Reducer>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer) {
    return map_reduce_cf_raw(ctx, name, init, mapper, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

template<class Mapper, class I, class Reducer, class Result>
future<I> map_reduce_cf_raw(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer, Result result) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([mapper, uuid](database& db) {
        return mapper(db.find_column_family(uuid));
    }, init, reducer);
}


template<class Mapper, class I, class Reducer, class Result>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer, Result result) {
    return map_reduce_cf_raw(ctx, name, init, mapper, reducer, result).then([result](const I& res) mutable {
        result = res;
        return make_ready_future<json::json_return_type>(result);
    });
}

template<class Mapper, class I, class Reducer>
future<I> map_reduce_cf_raw(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    return ctx.db.map_reduce0([mapper, init, reducer](database& db) {
        auto res = init;
        for (auto i : db.get_column_families()) {
            res = reducer(res, mapper(*i.second.get()));
        }
        return res;
    }, init, reducer);
}


template<class Mapper, class I, class Reducer>
future<json::json_return_type> map_reduce_cf(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    return map_reduce_cf_raw(ctx, init, mapper, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

future<json::json_return_type>  get_cf_stats(http_context& ctx, const sstring& name,
        int64_t column_family::stats::*f);

future<json::json_return_type>  get_cf_stats(http_context& ctx,
        int64_t column_family::stats::*f);

}
