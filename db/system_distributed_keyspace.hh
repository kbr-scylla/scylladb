/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "bytes.hh"
#include "schema_fwd.hh"
#include "service/migration_manager.hh"
#include "service/qos/qos_common.hh"
#include "utils/UUID.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace cql3 {
class query_processor;
}

namespace cdc {
    class stream_id;
    class topology_description;
    class streams_version;
} // namespace cdc

namespace service {
    class storage_proxy;
}

namespace db {

class system_distributed_keyspace {
public:
    static constexpr auto NAME = "system_distributed";
    static constexpr auto VIEW_BUILD_STATUS = "view_build_status";
    static constexpr auto SERVICE_LEVELS = "service_levels";

    /* Nodes use this table to communicate new CDC stream generations to other nodes. */
    static constexpr auto CDC_TOPOLOGY_DESCRIPTION = "cdc_generation_descriptions";

    /* This table is used by CDC clients to learn about available CDC streams. */
    static constexpr auto CDC_DESC_V2 = "cdc_streams_descriptions_v2";

    /* Used by CDC clients to learn CDC generation timestamps. */
    static constexpr auto CDC_TIMESTAMPS = "cdc_generation_timestamps";

    /* Previous version of the "cdc_streams_descriptions_v2" table.
     * We use it in the upgrade procedure to ensure that CDC generations appearing
     * in the old table also appear in the new table, if necessary. */
    static constexpr auto CDC_DESC_V1 = "cdc_streams_descriptions";

    /* Information required to modify/query some system_distributed tables, passed from the caller. */
    struct context {
        /* How many different token owners (endpoints) are there in the token ring? */
        size_t num_token_owners;
    };
private:
    cql3::query_processor& _qp;
    service::migration_manager& _mm;
    service::storage_proxy& _sp;

    bool _started = false;
    bool _forced_cdc_timestamps_schema_sync = false;

public:
    /* Should writes to the given table always be synchronized by commitlog (flushed to disk)
     * before being acknowledged? */
    static bool is_extra_durable(const sstring& cf_name);

    system_distributed_keyspace(cql3::query_processor&, service::migration_manager&, service::storage_proxy&);

    future<> start();
    future<> start_workload_prioritization();
    future<> stop();

    bool started() const { return _started; }

    future<std::unordered_map<utils::UUID, sstring>> view_status(sstring ks_name, sstring view_name) const;
    future<> start_view_build(sstring ks_name, sstring view_name) const;
    future<> finish_view_build(sstring ks_name, sstring view_name) const;
    future<> remove_view(sstring ks_name, sstring view_name) const;

    future<> insert_cdc_topology_description(db_clock::time_point streams_ts, const cdc::topology_description&, context);
    future<std::optional<cdc::topology_description>> read_cdc_topology_description(db_clock::time_point streams_ts, context);
    future<> expire_cdc_topology_description(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);

    future<> create_cdc_desc(db_clock::time_point streams_ts, const cdc::topology_description&, context);
    future<> expire_cdc_desc(db_clock::time_point streams_ts, db_clock::time_point expiration_time, context);
    future<bool> cdc_desc_exists(db_clock::time_point streams_ts, context);

    /* Get all generation timestamps appearing in the "cdc_streams_descriptions" table
     * (the old CDC stream description table). */
    future<std::vector<db_clock::time_point>> get_cdc_desc_v1_timestamps(context);

    future<std::map<db_clock::time_point, cdc::streams_version>> cdc_get_versioned_streams(db_clock::time_point not_older_than, context);
    future<std::map<db_clock::time_point, cdc::streams_version>> cdc_get_versioned_streams(context);

    future<qos::service_levels_info> get_service_levels() const;
    future<qos::service_levels_info> get_service_level(sstring service_level_name) const;
    future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const;
    future<> drop_service_level(sstring service_level_name) const;
    bool workload_prioritization_tables_exists();
private:
    future<> create_tables(std::vector<schema_ptr> tables);
};

}
