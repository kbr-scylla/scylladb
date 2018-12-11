
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include <boost/test/unit_test.hpp>

#include <seastar/util/defer.hh>

#include "tests/test-utils.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include <seastar/core/reactor.hh>
#include "service/storage_service.hh"
#include <seastar/core/distributed.hh>
#include "database.hh"
#include "db/system_distributed_keyspace.hh"

SEASTAR_TEST_CASE(test_boot_shutdown){
    return seastar::async([] {
        distributed<database> db;
        sharded<auth::service> auth_service;
        sharded<db::system_distributed_keyspace> sys_dist_ks;
        utils::fb_utilities::set_broadcast_address(gms::inet_address("127.0.0.1"));
        sharded<gms::feature_service> feature_service;
        feature_service.start().get();
        auto stop_feature_service = defer([&] { feature_service.stop().get(); });

        locator::i_endpoint_snitch::create_snitch("SimpleSnitch").get();
        auto stop_snitch = defer([&] { gms::get_failure_detector().stop().get(); });

        netw::get_messaging_service().start(gms::inet_address("127.0.0.1"), 7000, false /* don't bind */).get();
        auto stop_messaging_service = defer([&] { netw::get_messaging_service().stop().get(); });

        service::get_storage_service().start(std::ref(db), std::ref(auth_service), std::ref(sys_dist_ks), std::ref(feature_service)).get();
        auto stop_ss = defer([&] { service::get_storage_service().stop().get(); });

        db.start().get();
        auto stop_db = defer([&] { db.stop().get(); });

        gms::get_failure_detector().start().get();
        auto stop_failure_detector = defer([&] { gms::get_failure_detector().stop().get(); });

        gms::get_gossiper().start(std::ref(feature_service)).get();
        auto stop_gossiper = defer([&] { gms::get_gossiper().stop().get(); });
    });
}
