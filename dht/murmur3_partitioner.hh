/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"
#include <vector>

namespace dht {

class murmur3_partitioner final : public i_partitioner {
    unsigned _sharding_ignore_msb_bits;
    std::vector<uint64_t> _shard_start;
public:
    murmur3_partitioner(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0);
    virtual const sstring name() const { return "org.apache.cassandra.dht.Murmur3Partitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override;
    virtual int tri_compare(token_view t1, token_view t2) const override;
    virtual token midpoint(const token& t1, const token& t2) const override;
    virtual sstring to_sstring(const dht::token& t) const override;
    virtual dht::token from_sstring(const sstring& t) const override;
    virtual dht::token from_bytes(bytes_view bytes) const override;

    virtual unsigned shard_of(const token& t) const override;
    virtual unsigned shard_of(const token& t, unsigned shard_count, unsigned sharding_ignore_msb) const override;
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans) const override;
    virtual unsigned sharding_ignore_msb() const override;
};


}

