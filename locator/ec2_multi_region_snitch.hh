/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "locator/ec2_snitch.hh"

namespace locator {
class ec2_multi_region_snitch : public ec2_snitch {
public:
    ec2_multi_region_snitch(const sstring& fname = "", unsigned io_cpu_id = 0);
    virtual future<> gossiper_starting() override;
    virtual future<> start() override;
    virtual void set_local_private_addr(const sstring& addr_str) override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.Ec2MultiRegionSnitch";
    }
private:
    sstring _local_private_address;
};
} // namespace locator
