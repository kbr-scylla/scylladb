/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "i_partitioner.hh"
#include "bytes.hh"

#include "sstables/key.hh"

namespace dht {

class random_partitioner final : public i_partitioner {
public:
    random_partitioner(unsigned shard_count = smp::count, unsigned ignore_msb = 0) : i_partitioner(shard_count) {}
    virtual const sstring name() const { return "org.apache.cassandra.dht.RandomPartitioner"; }
    virtual token get_token(const schema& s, partition_key_view key) const override;
    virtual token get_token(const sstables::key_view& key) const override;
    virtual token get_random_token() override;
    virtual bool preserves_order() override { return false; }
    virtual std::map<token, float> describe_ownership(const std::vector<token>& sorted_tokens) override;
    virtual data_type get_token_validator() override { return varint_type; }
    virtual bytes token_to_bytes(const token& t) const override;
    virtual int tri_compare(token_view t1, token_view t2) const override;
    virtual token midpoint(const token& t1, const token& t2) const;
    virtual sstring to_sstring(const dht::token& t) const override;
    virtual dht::token from_sstring(const sstring& t) const override;
    virtual dht::token from_bytes(bytes_view bytes) const override;
    virtual unsigned shard_of(const token& t) const override;
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans) const override;
private:
    token get_token(bytes data) const;
};

}
