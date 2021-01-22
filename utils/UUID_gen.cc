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
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "UUID_gen.hh"
#ifdef __linux__
#include <net/if.h>
#include <sys/ioctl.h>
#include <net/if_arp.h>
#endif // __linux__

#include <stdlib.h>
#include "hashers.hh"

namespace utils {

static int64_t make_thread_local_node(int64_t node) {
    // An atomic counter to issue thread identifiers.
    // We should take current core number into consideration
    // because create_time_safe() doesn't synchronize across cores and
    // it's easy to get duplicates. Use an own counter since
    // seastar::this_shard_id() may not yet be available.
    static std::atomic<int64_t> thread_id_counter;
    static thread_local int64_t thread_id = thread_id_counter.fetch_add(1);
    // Mix in the core number into Organisational Unique
    // Identifier, to leave NIC intact, assuming tampering
    // with NIC is more likely to lead to collision within
    // a single network than tampering with OUI.
    //
    // Make sure the result fits into 6 bytes reserved for MAC
    // (adding the core number may overflow the original
    // value).
    return (node + (thread_id << 32)) & 0xFFFF'FFFF'FFFFL;
}

static int64_t make_random_node() {
    static int64_t random_node = [] {
        int64_t node = 0;
        std::random_device rndgen;
        do {
            auto i = rndgen();
            node = i;
            if (sizeof(i) < sizeof(node)) {
                node = (node << 32) + rndgen();
            }
        } while (node == 0); // 0 may mean "node is uninitialized", so avoid it.
        return node;
    }();
    return random_node;
}

static int64_t make_node() {
    static int64_t global_node = [] {
        int64_t node = 0;
#ifdef __linux__
        int fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd >= 0) {
            // Get a hardware address for an interface, if there is more than one, use any
            struct ifreq ifr_list[32];
            struct ifconf ifc;

            ifc.ifc_req = ifr_list;
            ifc.ifc_len = sizeof(ifr_list)/sizeof(ifr_list[0]);
            if (ioctl(fd, SIOCGIFCONF, static_cast<void*>(&ifc)) >= 0) {
                for (struct ifreq *ifr = ifr_list; ifr < ifr_list + ifc.ifc_len; ifr++) {
                    // Go over available addresses and pick any
                    // valid one, except loopback
                    if (ioctl(fd, SIOCGIFFLAGS, ifr) < 0) {
                        continue;
                    }
                    if (ifr->ifr_flags & IFF_LOOPBACK) { // don't count loopback
                        continue;
                    }
                    if (ioctl(fd, SIOCGIFHWADDR, ifr) < 0) {
                        continue;
                    }
                    auto macaddr = ifr->ifr_hwaddr.sa_data;
                    for (auto c = macaddr; c < macaddr + 6; c++) {
                        // Avoid little-big-endian differences
                        node = (node << 8) + static_cast<unsigned char>(*c);
                    }
                    if (node) {
                        break; // Success
                    }
                }
            }
            close(fd);
        }
#endif
        if (node == 0) {
            node = make_random_node();
        }
        return node;
    }();
    return make_thread_local_node(global_node);
}

static int64_t make_clock_seq_and_node()
{
    // The original Java code did this, shuffling the number of millis
    // since the epoch, and taking 14 bits of it. We don't do exactly
    // the same, but the idea is the same.
    //long clock = new Random(System.currentTimeMillis()).nextLong();
    unsigned int seed = std::chrono::system_clock::now().time_since_epoch().count();
    int clock = rand_r(&seed);

    long lsb = 0;
    lsb |= 0x8000000000000000L;                 // variant (2 bits)
    lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
    lsb |= make_node();                          // 6 bytes
    return lsb;
}

UUID UUID_gen::get_name_UUID(bytes_view b) {
    return get_name_UUID(reinterpret_cast<const unsigned char*>(b.begin()), b.size());
}

UUID UUID_gen::get_name_UUID(sstring_view s) {
    static_assert(sizeof(char) == sizeof(sstring_view::value_type), "Assumed that str.size() counts in chars");
    return get_name_UUID(reinterpret_cast<const unsigned char*>(s.begin()), s.size());
}

UUID UUID_gen::get_name_UUID(const unsigned char *s, size_t len) {
    bytes digest = md5_hasher::calculate(std::string_view(reinterpret_cast<const char*>(s), len));

    // set version to 3
    digest[6] &= 0x0f;
    digest[6] |= 0x30;

    // set variant to IETF variant
    digest[8] &= 0x3f;
    digest[8] |= 0x80;

    return get_UUID(digest);
}

const thread_local int64_t UUID_gen::spoof_node = make_thread_local_node(make_random_node());
const thread_local int64_t UUID_gen::clock_seq_and_node = make_clock_seq_and_node();
thread_local const std::unique_ptr<UUID_gen> UUID_gen::instance (new UUID_gen());


} // namespace utils
