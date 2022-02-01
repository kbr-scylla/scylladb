/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <optional>

#include "bytes.hh"
#include "keys.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "db/read_repair_decision.hh"

namespace service {

namespace pager {

class paging_state final {
public:
    using replicas_per_token_range = std::unordered_map<dht::token_range, std::vector<utils::UUID>>;

private:
    partition_key _partition_key;
    std::optional<clustering_key> _clustering_key;
    uint32_t _remaining_low_bits;
    utils::UUID _query_uuid;
    replicas_per_token_range _last_replicas;
    std::optional<db::read_repair_decision> _query_read_repair_decision;
    uint32_t _rows_fetched_for_last_partition_low_bits;
    uint32_t _remaining_high_bits;
    uint32_t _rows_fetched_for_last_partition_high_bits;

public:
    paging_state(partition_key pk,
            std::optional<clustering_key> ck,
            uint32_t rem,
            utils::UUID reader_recall_uuid,
            replicas_per_token_range last_replicas,
            std::optional<db::read_repair_decision> query_read_repair_decision,
            uint32_t rows_fetched_for_last_partition,
            uint32_t remaining_ext,
            uint32_t rows_fetched_for_last_partition_high_bits);

    paging_state(partition_key pk,
            std::optional<clustering_key> ck,
            uint64_t rem,
            utils::UUID reader_recall_uuid,
            replicas_per_token_range last_replicas,
            std::optional<db::read_repair_decision> query_read_repair_decision,
            uint64_t rows_fetched_for_last_partition);

    void set_partition_key(partition_key pk) {
        _partition_key = std::move(pk);
    }

    void set_clustering_key(clustering_key ck) {
        _clustering_key = std::move(ck);
    }

    void set_remaining(uint64_t remaining) {
        _remaining_low_bits = static_cast<uint32_t>(remaining);
        _remaining_high_bits = static_cast<uint32_t>(remaining >> 32);
    }

    /**
     * Last processed key, i.e. where to start from in next paging round
     */
    const partition_key& get_partition_key() const {
        return _partition_key;
    }
    /**
     * Clustering key in last partition. I.e. first, next, row
     */
    const std::optional<clustering_key>& get_clustering_key() const {
        return _clustering_key;
    }
    /**
     * Max remaining rows to fetch in total.
     * I.e. initial row_limit - #rows returned so far.
     */
    uint32_t get_remaining_low_bits() const {
        return _remaining_low_bits;
    }

    uint32_t get_rows_fetched_for_last_partition_low_bits() const {
        return _rows_fetched_for_last_partition_low_bits;
    }
    uint32_t get_remaining_high_bits() const {
        return _remaining_high_bits;
    }

    uint32_t get_rows_fetched_for_last_partition_high_bits() const {
        return _rows_fetched_for_last_partition_high_bits;
    }

    uint64_t get_remaining() const {
        return (static_cast<uint64_t>(_remaining_high_bits) << 32) | _remaining_low_bits;
    }

    uint64_t get_rows_fetched_for_last_partition() const {
        return (static_cast<uint64_t>(_rows_fetched_for_last_partition_high_bits) << 32) | _rows_fetched_for_last_partition_low_bits;
    }

    /**
     * query_uuid is a unique key under which the replicas saved the
     * readers used to serve the last page. These saved readers may be
     * resumed (if not already purged from the cache) instead of opening new
     * ones - as a performance optimization.
     * If the uuid is the invalid default-constructed UUID(), it means that
     * the client got this paging_state from a coordinator running an older
     * version of Scylla.
     */
    utils::UUID get_query_uuid() const {
        return _query_uuid;
    }

    /**
     * The replicas used to serve the last page.
     *
     * Helps paged queries consistently hit the same replicas for each
     * subsequent page. Replicas that already served a page will keep
     * the readers used for filling it around in a cache. Subsequent
     * page request hitting the same replicas can reuse these readers
     * to fill the pages avoiding the work of creating these readers
     * from scratch on every page.
     * In a mixed cluster older coordinators will ignore this value.
     * Replicas are stored per token-range where the token-range
     * is some subrange of the query range that doesn't cross node
     * boundaries.
     */
    replicas_per_token_range get_last_replicas() const {
        return _last_replicas;
    }

    /**
     * The read-repair decision made for this query.
     *
     * The read-repair decision is made on the first page and is applied to
     * all subsequent pages. This way we can ensure that we consistently use
     * the same replicas on each page throughout the query. This helps
     * reader-reuse on the replicas as readers can only be reused if they're
     * used for all pages, if the replica is skipped for one or more pages the
     * saved reader has to be dropped.
     */
    std::optional<db::read_repair_decision> get_query_read_repair_decision() const {
        return _query_read_repair_decision;
    }

    static lw_shared_ptr<paging_state> deserialize(bytes_opt bytes);
    bytes_opt serialize() const;
};

}

}
