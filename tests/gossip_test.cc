
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include <boost/test/unit_test.hpp>

#include "tests/test-utils.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "core/reactor.hh"
#include "service/storage_service.hh"
#include "core/distributed.hh"
#include "database.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        sharded<auth::service> auth_service;
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
        service::get_storage_service().start(std::ref(db), std::ref(auth_service)).get();
        db.start().get();
        netw::get_messaging_service().start(gms::inet_address("127.0.0.1")).get();
        gms::get_failure_detector().start().get();

        gms::get_gossiper().start().get();
        gms::get_gossiper().stop().get();
        gms::get_failure_detector().stop().get();
        db.stop().get();
        service::get_storage_service().stop().get();
        netw::get_messaging_service().stop().get();
        locator::i_endpoint_snitch::stop_snitch().get();
    });
}
