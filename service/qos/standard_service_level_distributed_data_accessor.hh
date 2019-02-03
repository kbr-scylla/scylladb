/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "seastarx.hh"
#include "service_level_controller.hh"


namespace db {
    class system_distributed_keyspace;
}
namespace qos {
class standard_service_level_distributed_data_accessor : public service_level_controller::service_level_distributed_data_accessor,
         ::enable_shared_from_this<standard_service_level_distributed_data_accessor> {
private:
    db::system_distributed_keyspace& _sys_dist_ks;
public:
    standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks);
    virtual future<qos::service_levels_info> get_service_levels() const override;
    virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override;
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const override;
    virtual future<> drop_service_level(sstring service_level_name) const override;
};
}
