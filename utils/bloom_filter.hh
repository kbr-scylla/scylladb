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
 *
 * This file contains code from a variety of .java files from the
 * Cassandra project, adapted to our needs.
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#pragma once
#include "i_filter.hh"
#include "utils/murmur_hash.hh"
#include "utils/large_bitset.hh"

#include <vector>

namespace utils {
namespace filter {

class bloom_filter: public i_filter {
public:
    using bitmap = large_bitset;

private:
    bitmap _bitset;
    int _hash_count;
public:
    int num_hashes() { return _hash_count; }
    bitmap& bits() { return _bitset; }

    bloom_filter(int hashes, bitmap&& bs) : _bitset(std::move(bs)), _hash_count(hashes) {
    }

    virtual void add(const bytes_view& key) override;

    virtual bool is_present(const bytes_view& key) override;

    virtual bool is_present(hashed_key key) override;

    virtual void clear() override {
        _bitset.clear();
    }

    virtual void close() override { }

    virtual size_t memory_size() override {
        return sizeof(_hash_count) + _bitset.memory_size();
    }
};

struct murmur3_bloom_filter: public bloom_filter {

    murmur3_bloom_filter(int hashes, bitmap&& bs) : bloom_filter(hashes, std::move(bs)) {}

};

struct always_present_filter: public i_filter {

    virtual bool is_present(const bytes_view& key) override {
        return true;
    }

    virtual bool is_present(hashed_key key) override {
        return true;
    }

    virtual void add(const bytes_view& key) override { }

    virtual void clear() override { }

    virtual void close() override { }

    virtual size_t memory_size() override {
        return 0;
    }
};

filter_ptr create_filter(int hash, large_bitset&& bitset);
filter_ptr create_filter(int hash, int64_t num_elements, int buckets_per);
}
}
