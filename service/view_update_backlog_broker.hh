/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "gms/i_endpoint_state_change_subscriber.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace gms {
class gossiper;
}

namespace service {

class storage_proxy;

class view_update_backlog_broker final
        : public seastar::peering_sharded_service<view_update_backlog_broker>
        , public seastar::async_sharded_service<view_update_backlog_broker>
        , public gms::i_endpoint_state_change_subscriber {

    seastar::sharded<storage_proxy>& _sp;
    gms::gossiper& _gossiper;
    seastar::future<> _started = make_ready_future<>();
    seastar::abort_source _as;

public:
    view_update_backlog_broker(seastar::sharded<storage_proxy>&, gms::gossiper&);

    seastar::future<> start();

    seastar::future<> stop();

    virtual void on_change(gms::inet_address, gms::application_state, const gms::versioned_value&) override;

    virtual void on_remove(gms::inet_address) override;

    virtual void on_join(gms::inet_address, gms::endpoint_state) override { }
    virtual void before_change(gms::inet_address, gms::endpoint_state, gms::application_state, const gms::versioned_value&) override { }
    virtual void on_alive(gms::inet_address, gms::endpoint_state) override { }
    virtual void on_dead(gms::inet_address, gms::endpoint_state) override { }
    virtual void on_restart(gms::inet_address, gms::endpoint_state) override { }
};

}
