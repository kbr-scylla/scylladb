/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


/* This module contains classes and functions used to manage CDC generations:
 * sets of CDC stream identifiers used by the cluster to choose partition keys for CDC log writes.
 * Each CDC generation begins operating at a specific time point, called the generation's timestamp.
 * The generation is used by all nodes in the cluster to pick CDC streams until superseded by a new generation.
 *
 * Functions from this module are used by the node joining procedure to introduce new CDC generations to the cluster
 * (which is necessary due to new tokens being inserted into the token ring), or during rolling upgrade
 * if CDC is enabled for the first time.
 */

#pragma once

#include <vector>
#include <unordered_set>
#include <seastar/util/noncopyable_function.hh>

#include "database_fwd.hh"
#include "db_clock.hh"
#include "dht/token.hh"
#include "locator/token_metadata.hh"
#include "utils/chunked_vector.hh"
#include "cdc/generation_id.hh"

namespace seastar {
    class abort_source;
} // namespace seastar

namespace db {
    class config;
    class system_distributed_keyspace;
} // namespace db

namespace gms {
    class inet_address;
    class gossiper;
} // namespace gms

namespace cdc {

class stream_id final {
    bytes _value;
public:
    static constexpr uint8_t version_1 = 1;

    stream_id() = default;
    stream_id(bytes);
    stream_id(dht::token, size_t);

    bool is_set() const;
    bool operator==(const stream_id&) const;
    bool operator!=(const stream_id&) const;
    bool operator<(const stream_id&) const;

    uint8_t version() const;
    size_t index() const;
    const bytes& to_bytes() const;
    dht::token token() const;

    partition_key to_partition_key(const schema& log_schema) const;
    static int64_t token_from_bytes(bytes_view);
};

/* Describes a mapping of tokens to CDC streams in a token range.
 *
 * The range ends with `token_range_end`. A vector of `token_range_description`s defines the ranges entirely
 * (the end of the `i`th range is the beginning of the `i+1 % size()`th range). Ranges are left-opened, right-closed.
 *
 * Tokens in the range ending with `token_range_end` are mapped to streams in the `streams` vector as follows:
 * token `T` is mapped to `streams[j]` if and only if the used partitioner maps `T` to the `j`th shard,
 * assuming that the partitioner is configured for `streams.size()` shards and (partitioner's) `sharding_ignore_msb`
 * equals to the given `sharding_ignore_msb`.
*/
struct token_range_description {
    dht::token token_range_end;
    std::vector<stream_id> streams;
    uint8_t sharding_ignore_msb;

    bool operator==(const token_range_description&) const;
};


/* Describes a mapping of tokens to CDC streams in a whole token ring.
 *
 * Division of the ring to token ranges is defined in terms of `token_range_end`s
 * in the `_entries` vector. See the comment above `token_range_description` for explanation.
 */
class topology_description {
    std::vector<token_range_description> _entries;
public:
    topology_description(std::vector<token_range_description> entries);
    bool operator==(const topology_description&) const;

    const std::vector<token_range_description>& entries() const&;
    std::vector<token_range_description>&& entries() &&;
};

/**
 * The set of streams for a single topology version/generation
 * I.e. the stream ids at a given time. 
 */ 
class streams_version {
public:
    utils::chunked_vector<stream_id> streams;
    db_clock::time_point timestamp;

    streams_version(utils::chunked_vector<stream_id> s, db_clock::time_point ts)
        : streams(std::move(s))
        , timestamp(ts)
    {}
};

class no_generation_data_exception : public std::runtime_error {
public:
    no_generation_data_exception(cdc::generation_id generation_ts)
        : std::runtime_error(format("could not find generation data for timestamp {}", generation_ts))
    {}
};

/* Should be called when we're restarting and we noticed that we didn't save any streams timestamp in our local tables,
 * which means that we're probably upgrading from a non-CDC/old CDC version (another reason could be
 * that there's a bug, or the user messed with our local tables).
 *
 * It checks whether we should be the node to propose the first generation of CDC streams.
 * The chosen condition is arbitrary, it only tries to make sure that no two nodes propose a generation of streams
 * when upgrading, and nothing bad happens if they for some reason do (it's mostly an optimization).
 */
bool should_propose_first_generation(const gms::inet_address& me, const gms::gossiper&);

/* Generate a new set of CDC streams and insert it into the distributed cdc_generation_descriptions table.
 * Returns the timestamp of this new generation
 *
 * Should be called when starting the node for the first time (i.e., joining the ring).
 *
 * Assumes that the system_distributed keyspace is initialized.
 *
 * The caller of this function is expected to insert this timestamp into the gossiper as fast as possible,
 * so that other nodes learn about the generation before their clocks cross the timestmap
 * (not guaranteed in the current implementation, but expected to be the common case;
 *  we assume that `ring_delay` is enough for other nodes to learn about the new generation).
 */
future<cdc::generation_id> make_new_cdc_generation(
        const db::config& cfg,
        const std::unordered_set<dht::token>& bootstrap_tokens,
        const locator::token_metadata_ptr tmptr,
        const gms::gossiper& g,
        db::system_distributed_keyspace& sys_dist_ks,
        std::chrono::milliseconds ring_delay,
        bool add_delay);

/* Part of the upgrade procedure. Useful in case where the version of Scylla that we're upgrading from
 * used the "cdc_streams_descriptions" table. This procedure ensures that the new "cdc_streams_descriptions_v2"
 * table contains streams of all generations that were present in the old table and may still contain data
 * (i.e. there exist CDC log tables that may contain rows with partition keys being the stream IDs from
 * these generations). */
future<> maybe_rewrite_streams_descriptions(
        const database&,
        shared_ptr<db::system_distributed_keyspace>,
        noncopyable_function<unsigned()> get_num_token_owners,
        abort_source&);

} // namespace cdc
