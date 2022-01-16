/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "qos_common.hh"


namespace qos {

    struct service_level_info {
        sstring name;
        seastar::scheduling_group sg;
        io_priority_class pc;
    };
    class qos_configuration_change_subscriber {
    public:
        /** This callback is going to be called just before the service level is available **/
        virtual future<> on_before_service_level_add(service_level_options slo, service_level_info sl_info) = 0;
        /** This callback is going to be called just after the service level is removed **/
        virtual future<> on_after_service_level_remove(service_level_info sl_info) = 0;
        /** This callback is going to be called just before the service level is changed **/
        virtual future<> on_before_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) = 0;

        virtual ~qos_configuration_change_subscriber() {};
    };
}
