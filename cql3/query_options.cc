/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "query_options.hh"
#include "version.hh"

namespace cql3 {

thread_local const query_options::specific_options query_options::specific_options::DEFAULT{-1, {}, {}, api::missing_timestamp};

thread_local query_options query_options::DEFAULT{db::consistency_level::ONE, infinite_timeout_config, std::experimental::nullopt,
    std::vector<cql3::raw_value_view>(), false, query_options::specific_options::DEFAULT, cql_serialization_format::latest()};

query_options::query_options(db::consistency_level consistency,
                           const ::timeout_config& timeout_config,
                           std::experimental::optional<std::vector<sstring_view>> names,
                           std::vector<cql3::raw_value> values,
                           std::vector<cql3::raw_value_view> value_views,
                           bool skip_metadata,
                           specific_options options,
                           cql_serialization_format sf)
   : _consistency(consistency)
   , _timeout_config(timeout_config)
   , _names(std::move(names))
   , _values(std::move(values))
   , _value_views(value_views)
   , _skip_metadata(skip_metadata)
   , _options(std::move(options))
   , _cql_serialization_format(sf)
{
}

query_options::query_options(db::consistency_level consistency,
                             const ::timeout_config& timeout_config,
                             std::experimental::optional<std::vector<sstring_view>> names,
                             std::vector<cql3::raw_value> values,
                             bool skip_metadata,
                             specific_options options,
                             cql_serialization_format sf)
    : _consistency(consistency)
    , _timeout_config(timeout_config)
    , _names(std::move(names))
    , _values(std::move(values))
    , _value_views()
    , _skip_metadata(skip_metadata)
    , _options(std::move(options))
    , _cql_serialization_format(sf)
{
    fill_value_views();
}

query_options::query_options(db::consistency_level consistency,
                             const ::timeout_config& timeout_config,
                             std::experimental::optional<std::vector<sstring_view>> names,
                             std::vector<cql3::raw_value_view> value_views,
                             bool skip_metadata,
                             specific_options options,
                             cql_serialization_format sf)
    : _consistency(consistency)
    , _timeout_config(timeout_config)
    , _names(std::move(names))
    , _values()
    , _value_views(std::move(value_views))
    , _skip_metadata(skip_metadata)
    , _options(std::move(options))
    , _cql_serialization_format(sf)
{
}

query_options::query_options(db::consistency_level cl, const ::timeout_config& timeout_config, std::vector<cql3::raw_value> values, specific_options options)
    : query_options(
          cl,
          timeout_config,
          {},
          std::move(values),
          false,
          std::move(options),
          cql_serialization_format::latest()
      )
{
}

query_options::query_options(std::unique_ptr<query_options> qo, ::shared_ptr<service::pager::paging_state> paging_state)
        : query_options(qo->_consistency,
        qo->get_timeout_config(),
        std::move(qo->_names),
        std::move(qo->_values),
        std::move(qo->_value_views),
        qo->_skip_metadata,
        std::move(query_options::specific_options{qo->_options.page_size, paging_state, qo->_options.serial_consistency, qo->_options.timestamp}),
        qo->_cql_serialization_format) {

}

query_options::query_options(std::vector<cql3::raw_value> values)
    : query_options(
          db::consistency_level::ONE, infinite_timeout_config, std::move(values))
{}

db::consistency_level query_options::get_consistency() const
{
    return _consistency;
}

cql3::raw_value_view query_options::get_value_at(size_t idx) const
{
    return _value_views.at(idx);
}

size_t query_options::get_values_count() const
{
    return _value_views.size();
}

cql3::raw_value_view query_options::make_temporary(cql3::raw_value value) const
{
    if (value) {
        _temporaries.emplace_back(value->begin(), value->end());
        auto& temporary = _temporaries.back();
        return cql3::raw_value_view::make_value(bytes_view{temporary.data(), temporary.size()});
    }
    return cql3::raw_value_view::make_null();
}

bool query_options::skip_metadata() const
{
    return _skip_metadata;
}

int32_t query_options::get_page_size() const
{
    return get_specific_options().page_size;
}

::shared_ptr<service::pager::paging_state> query_options::get_paging_state() const
{
    return get_specific_options().state;
}

std::experimental::optional<db::consistency_level> query_options::get_serial_consistency() const
{
    return get_specific_options().serial_consistency;
}

api::timestamp_type query_options::get_timestamp(service::query_state& state) const
{
    auto tstamp = get_specific_options().timestamp;
    return tstamp != api::missing_timestamp ? tstamp : state.get_timestamp();
}

int query_options::get_protocol_version() const
{
    return _cql_serialization_format.protocol_version();
}

cql_serialization_format query_options::get_cql_serialization_format() const
{
    return _cql_serialization_format;
}

const query_options::specific_options& query_options::get_specific_options() const
{
    return _options;
}

const query_options& query_options::for_statement(size_t i) const
{
    if (!_batch_options) {
        // No per-statement options supplied, so use the "global" options
        return *this;
    }
    return _batch_options->at(i);
}

void query_options::prepare(const std::vector<::shared_ptr<column_specification>>& specs)
{
    if (!_names) {
        return;
    }

    auto& names = *_names;
    std::vector<cql3::raw_value> ordered_values;
    ordered_values.reserve(specs.size());
    for (auto&& spec : specs) {
        auto& spec_name = spec->name->text();
        for (size_t j = 0; j < names.size(); j++) {
            if (names[j] == spec_name) {
                ordered_values.emplace_back(_values[j]);
                break;
            }
        }
    }
    _values = std::move(ordered_values);
    fill_value_views();
}

void query_options::fill_value_views()
{
    for (auto&& value : _values) {
        if (value) {
            _value_views.emplace_back(cql3::raw_value_view::make_value(bytes_view{*value}));
        } else {
            _value_views.emplace_back(cql3::raw_value_view::make_null());
        }
    }
}

}
