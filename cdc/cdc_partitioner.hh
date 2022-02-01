/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "dht/i_partitioner.hh"

class schema;
class partition_key_view;

namespace sstables {

class key_view;

}

namespace cdc {

struct cdc_partitioner final : public dht::i_partitioner {
    cdc_partitioner() = default;
    virtual const sstring name() const override;
    virtual dht::token get_token(const schema& s, partition_key_view key) const override;
    virtual dht::token get_token(const sstables::key_view& key) const override;
};


}
