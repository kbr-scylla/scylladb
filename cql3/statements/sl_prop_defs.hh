/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "cql3/statements/property_definitions.hh"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <optional>
#include "timeout_config.hh"
#include "service/qos/qos_common.hh"

class keyspace_metadata;

namespace cql3 {

namespace statements {

class sl_prop_defs : public property_definitions {
    qos::service_level_options _slo;
public:
    void validate();

    static constexpr auto KW_SHARES = "shares";
    static constexpr int SHARES_DEFAULT_VAL = 1000;
    static constexpr int SHARES_MIN_VAL = 1;
    static constexpr int SHARES_MAX_VAL = 1000;

    qos::service_level_options get_service_level_options() const;
};

}

}
