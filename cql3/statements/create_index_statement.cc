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
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/coroutine.hh>
#include "create_index_statement.hh"
#include "prepared_statement.hh"
#include "validation.hh"
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "schema.hh"
#include "schema_builder.hh"
#include "request_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "index/target_parser.hh"
#include "gms/feature_service.hh"
#include "cql3/query_processor.hh"
#include "cql3/index_name.hh"
#include "cql3/statements/index_prop_defs.hh"
#include "index/secondary_index_manager.hh"
#include "mutation.hh"

#include <boost/range/adaptor/transformed.hpp>
#include <boost/algorithm/string/join.hpp>

namespace cql3 {

namespace statements {

create_index_statement::create_index_statement(cf_name name,
                                               ::shared_ptr<index_name> index_name,
                                               std::vector<::shared_ptr<index_target::raw>> raw_targets,
                                               ::shared_ptr<index_prop_defs> properties,
                                               bool if_not_exists)
    : schema_altering_statement(name)
    , _index_name(index_name->get_idx())
    , _raw_targets(raw_targets)
    , _properties(properties)
    , _if_not_exists(if_not_exists)
{
}

future<>
create_index_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return state.has_column_family_access(qp.db(), keyspace(), column_family(), auth::permission::ALTER);
}

void
create_index_statement::validate(query_processor& qp, const service::client_state& state) const
{
    if (_raw_targets.empty() && !_properties->is_custom) {
        throw exceptions::invalid_request_exception("Only CUSTOM indexes can be created without specifying a target column");
    }

    _properties->validate();
}

std::vector<::shared_ptr<index_target>> create_index_statement::validate_while_executing(query_processor& qp) const {
    auto db = qp.db();
    auto schema = validation::validate_column_family(db, keyspace(), column_family());

    if (schema->is_counter()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on counter tables");
    }

    if (schema->is_view()) {
        throw exceptions::invalid_request_exception("Secondary indexes are not supported on materialized views");
    }

    if (schema->is_dense()) {
        throw exceptions::invalid_request_exception(
                "Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");
    }

    validate_for_local_index(*schema);

    std::vector<::shared_ptr<index_target>> targets;
    for (auto& raw_target : _raw_targets) {
        targets.emplace_back(raw_target->prepare(*schema));
    }

    if (targets.size() > 1) {
        validate_targets_for_multi_column_index(targets);
    }

    for (auto& target : targets) {
        auto* ident = std::get_if<::shared_ptr<column_identifier>>(&target->value);
        if (!ident) {
            continue;
        }
        auto cd = schema->get_column_definition((*ident)->name());

        if (cd == nullptr) {
            throw exceptions::invalid_request_exception(
                    format("No column definition found for column {}", target->as_string()));
        }

        //NOTICE(sarna): Should be lifted after resolving issue #2963
        if (cd->is_static()) {
            throw exceptions::invalid_request_exception("Indexing static columns is not implemented yet.");
        }

        if (cd->type->references_duration()) {
            using request_validations::check_false;
            const auto& ty = *cd->type;

            check_false(ty.is_collection(), "Secondary indexes are not supported on collections containing durations");
            check_false(ty.is_user_type(), "Secondary indexes are not supported on UDTs containing durations");
            check_false(ty.is_tuple(), "Secondary indexes are not supported on tuples containing durations");

            // We're a duration.
            throw exceptions::invalid_request_exception("Secondary indexes are not supported on duration columns");
        }

        // Origin TODO: we could lift that limitation
        if ((schema->is_dense() || !schema->thrift().has_compound_comparator()) && cd->is_primary_key()) {
            throw exceptions::invalid_request_exception(
                    "Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
        }

        if (cd->kind == column_kind::partition_key && cd->is_on_all_components()) {
            throw exceptions::invalid_request_exception(
                    format("Cannot create secondary index on partition key column {}",
                            target->as_string()));
        }

        if (cd->type->is_multi_cell()) {
            // NOTICE(sarna): should be lifted after #2962 (indexes on non-frozen collections) is implemented
            // NOTICE(kbraun): don't forget about non-frozen user defined types
            throw exceptions::invalid_request_exception(
                    format("Cannot create secondary index on non-frozen collection or UDT column {}", cd->name_as_text()));
        } else if (cd->type->is_collection()) {
            validate_for_frozen_collection(*target);
        } else {
            validate_not_full_index(*target);
            validate_is_values_index_if_target_column_not_collection(cd, *target);
            validate_target_column_is_map_if_index_involves_keys(cd->type->is_map(), *target);
        }
    }

    if (db.existing_index_names(keyspace()).contains(_index_name)) {
        if (!_if_not_exists) {
            throw exceptions::invalid_request_exception("Index already exists");
        }
    }

    return targets;
}

void create_index_statement::validate_for_local_index(const schema& schema) const {
    if (!_raw_targets.empty()) {
            if (const auto* index_pk = std::get_if<std::vector<::shared_ptr<column_identifier::raw>>>(&_raw_targets.front()->value)) {
                auto base_pk_identifiers = *index_pk | boost::adaptors::transformed([&schema] (const ::shared_ptr<column_identifier::raw>& raw_ident) {
                    return raw_ident->prepare_column_identifier(schema);
                });
                auto remaining_base_pk_columns = schema.partition_key_columns();
                auto next_expected_base_column = remaining_base_pk_columns.begin();
                for (const auto& ident : base_pk_identifiers) {
                    auto it = schema.columns_by_name().find(ident->name());
                    if (it == schema.columns_by_name().end() || !it->second->is_partition_key()) {
                        throw exceptions::invalid_request_exception(format("Local index definition must contain full partition key only. Redundant column: {}", ident->to_string()));
                    }
                    if (next_expected_base_column == remaining_base_pk_columns.end()) {
                        throw exceptions::invalid_request_exception(format("Duplicate column definition in local index: {}", it->first));
                    }
                    if (&*next_expected_base_column != it->second) {
                        break;
                    }
                    ++next_expected_base_column;
                }
                if (next_expected_base_column != remaining_base_pk_columns.end()) {
                    throw exceptions::invalid_request_exception(format("Local index definition must contain full partition key only. Missing column: {}", next_expected_base_column->name_as_text()));
                }
                if (_raw_targets.size() == 1) {
                    throw exceptions::invalid_request_exception(format("Local index definition must provide an indexed column, not just partition key"));
                }
            }
        }
        for (unsigned i = 1; i < _raw_targets.size(); ++i) {
            if (std::holds_alternative<index_target::raw::multiple_columns>(_raw_targets[i]->value)) {
                throw exceptions::invalid_request_exception(format("Multi-column index targets are currently only supported for partition key"));
            }
        }
}

void create_index_statement::validate_for_frozen_collection(const index_target& target) const
{
    if (target.type != index_target::target_type::full) {
        throw exceptions::invalid_request_exception(
                format("Cannot create index on {} of frozen collection column {}",
                        index_target::index_option(target.type),
                        target.as_string()));
    }
}

void create_index_statement::validate_not_full_index(const index_target& target) const
{
    if (target.type == index_target::target_type::full) {
        throw exceptions::invalid_request_exception("full() indexes can only be created on frozen collections");
    }
}

void create_index_statement::validate_is_values_index_if_target_column_not_collection(
        const column_definition* cd, const index_target& target) const
{
    if (!cd->type->is_collection()
            && target.type != index_target::target_type::values) {
        throw exceptions::invalid_request_exception(
                format("Cannot create index on {} of column {}; only non-frozen collections support {} indexes",
                       index_target::index_option(target.type),
                       target.as_string(),
                       index_target::index_option(target.type)));
    }
}

void create_index_statement::validate_target_column_is_map_if_index_involves_keys(bool is_map, const index_target& target) const
{
    if (target.type == index_target::target_type::keys
            || target.type == index_target::target_type::keys_and_values) {
        if (!is_map) {
            throw exceptions::invalid_request_exception(
                    format("Cannot create index on {} of column {} with non-map type",
                           index_target::index_option(target.type), target.as_string()));
        }
    }
}

void create_index_statement::validate_targets_for_multi_column_index(std::vector<::shared_ptr<index_target>> targets) const
{
    if (!_properties->is_custom) {
        if (targets.size() > 2 || (targets.size() == 2 && std::holds_alternative<index_target::single_column>(targets.front()->value))) {
            throw exceptions::invalid_request_exception("Only CUSTOM indexes support multiple columns");
        }
    }
    std::unordered_set<sstring> columns;
    for (auto& target : targets) {
        if (columns.contains(target->as_string())) {
            throw exceptions::invalid_request_exception(format("Duplicate column {} in index target list", target->as_string()));
        }
        columns.emplace(target->as_string());
    }
}

schema_ptr create_index_statement::build_index_schema(query_processor& qp) const {
    auto targets = validate_while_executing(qp);

    data_dictionary::database db = qp.db();
    auto schema = db.find_schema(keyspace(), column_family());

    sstring accepted_name = _index_name;
    if (accepted_name.empty()) {
        std::optional<sstring> index_name_root;
        if (targets.size() == 1) {
           index_name_root = targets[0]->as_string();
        } else if ((targets.size() == 2 && std::holds_alternative<index_target::multiple_columns>(targets.front()->value))) {
            index_name_root = targets[1]->as_string();
        }
        accepted_name = db.get_available_index_name(keyspace(), column_family(), index_name_root);
    }
    index_metadata_kind kind;
    index_options_map index_options;
    if (_properties->is_custom) {
        kind = index_metadata_kind::custom;
        index_options = _properties->get_options();
    } else {
        kind = schema->is_compound() ? index_metadata_kind::composites : index_metadata_kind::keys;
    }
    auto index = make_index_metadata(targets, accepted_name, kind, index_options);
    auto existing_index = schema->find_index_noname(index);
    if (existing_index) {
        if (_if_not_exists) {
            return schema_ptr();
        } else {
            throw exceptions::invalid_request_exception(
                    format("Index {} is a duplicate of existing index {}", index.name(), existing_index.value().name()));
        }
    }
    auto index_table_name = secondary_index::index_table_name(accepted_name);
    if (db.has_schema(keyspace(), index_table_name)) {
        // We print this error even if _if_not_exists - in this case the user
        // asked to create a not-previously-existing index, but under an
        // already-taken name. This should be an error, not a silent success.
        throw exceptions::invalid_request_exception(format("Index {} cannot be created, because table {} already exists", accepted_name, index_table_name));
    }
    ++_cql_stats->secondary_index_creates;
    schema_builder builder{schema};
    builder.with_index(index);

    return builder.build();
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
create_index_statement::prepare_schema_mutations(query_processor& qp) const {
    using namespace cql_transport;
    auto schema = build_index_schema(qp);

    ::shared_ptr<event::schema_change> ret;
    std::vector<mutation> m;

    if (schema) {
        m = co_await qp.get_migration_manager().prepare_column_family_update_announcement(std::move(schema), false, {}, std::nullopt);

        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::UPDATED,
                event::schema_change::target_type::TABLE,
                keyspace(),
                column_family());
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
create_index_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    _cql_stats = &stats;
    return std::make_unique<prepared_statement>(audit_info(), make_shared<create_index_statement>(*this));
}

index_metadata create_index_statement::make_index_metadata(const std::vector<::shared_ptr<index_target>>& targets,
                                                           const sstring& name,
                                                           index_metadata_kind kind,
                                                           const index_options_map& options)
{
    index_options_map new_options = options;
    auto target_option = secondary_index::target_parser::serialize_targets(targets);
    new_options.emplace(index_target::target_option_name, target_option);

    const auto& first_target = targets.front()->value;
    return index_metadata{name, new_options, kind, index_metadata::is_local_index(std::holds_alternative<index_target::multiple_columns>(first_target))};
}

}

}
