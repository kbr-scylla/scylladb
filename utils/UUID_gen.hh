#pragma once
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

#include <stdint.h>
#include <assert.h>

#include <memory>
#include <chrono>
#include <random>
#include <limits>

#include "UUID.hh"
#include "db_clock.hh"

namespace utils {

// Scylla uses specialized timeuuids for list keys. They use
// limited space of timeuuid clockseq component to store
// sub-microsecond time. This exception is thrown when an attempt
// is made to construct such a UUID with a sub-microsecond argument
// which is outside the available bit range.
struct timeuuid_submicro_out_of_range: public std::out_of_range {
    using out_of_range::out_of_range;
};

/**
 * The goods are here: www.ietf.org/rfc/rfc4122.txt.
 */
class UUID_gen
{
private:
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    static constexpr int64_t START_EPOCH = -12219292800000L;
    // A random mac address for use in timeuuids
    // where we can not use clockseq to randomize the physical
    // node, and prefer using a random address to a physical one
    // to avoid duplicate timeuuids when system time goes back
    // while scylla is restarting. Using a spoof node also helps
    // avoid timeuuid duplicates when multiple nodes run on the
    // same host and share the physical MAC address.
    static thread_local const int64_t spoof_node;
    static thread_local const int64_t clock_seq_and_node;

    /*
     * The min and max possible lsb for a UUID.
     * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
     * compares the lsb parts as a signed byte array comparison. So the min
     * value is 8 times -128 and the max is 8 times +127.
     *
     * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
     * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
     * I don't think that has any practical consequence and is more robust in
     * case someone provides a UUID with a broken variant.
     */
    static constexpr int64_t MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
    static constexpr int64_t MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    static thread_local const std::unique_ptr<UUID_gen> instance;

    uint64_t last_nanos = 0;

    UUID_gen()
    {
        // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
        assert(clock_seq_and_node != 0);
    }

public:
    // We have only 17 timeuuid bits available to store this
    // value.
    static constexpr int SUBMICRO_LIMIT = (1<<17);
    // UUID timestamp time component is represented in intervals
    // of 1/10 of a microsecond since the beginning of GMT epoch.
    using decimicroseconds = std::chrono::duration<int64_t, std::ratio<1, 10'000'000>>;
    /**
     * Creates a type 1 UUID (time-based UUID).
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID()
    {
        auto uuid = UUID(instance->create_time_safe(), clock_seq_and_node);
        assert(uuid.is_timestamp());
        return uuid;
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID(int64_t when)
    {
        auto uuid = UUID(create_time(from_unix_timestamp(when)), clock_seq_and_node);
        assert(uuid.is_timestamp());
        return uuid;
    }

    /**
     * Creates a type 1 UUID (time-based UUID) with the wall clock time point @param tp.
     *
     * @return a UUID instance
     */
    static UUID get_time_UUID(std::chrono::system_clock::time_point tp)
    {
        using namespace std::chrono;
        // "nanos" needs to be in 100ns intervals since the adoption of the Gregorian calendar in the West.
        uint64_t nanos = duration_cast<nanoseconds>(tp.time_since_epoch()).count() / 100;
        nanos -= (10000ULL * START_EPOCH);
        auto uuid = UUID(create_time(nanos), clock_seq_and_node);
        assert(uuid.is_timestamp());
        return uuid;
    }

    static UUID get_time_UUID(int64_t when, int64_t clock_seq_and_node)
    {
        auto uuid = UUID(create_time(from_unix_timestamp(when)), clock_seq_and_node);
        assert(uuid.is_timestamp());
        return uuid;
    }

    static UUID get_time_UUID_raw(int64_t nanos, int64_t clock_seq_and_node)
    {
        auto uuid = UUID(create_time(nanos), clock_seq_and_node);
        assert(uuid.is_timestamp());
        return uuid;
    }

    /**
     * Similar to get_time_UUID, but randomize the clock and sequence.
     * If you can guarantee that the when_in_micros() argument is unique for
     * every call, then you should prefer get_time_UUID_from_micros() which is faster. If you can't
     * guarantee this however, this method will ensure the returned UUID are still unique (across calls)
     * through randomization.
     *
     * @param when_in_micros a unix time in microseconds.
     * @return a new UUID 'id' such that micros_timestamp(id) == when_in_micros. The UUID returned
     * by different calls will be unique even if when_in_micros is not.
     */
    static UUID get_random_time_UUID_from_micros(int64_t when_in_micros) {
        static thread_local std::mt19937_64 rand_gen(std::random_device().operator()());
        static thread_local std::uniform_int_distribution<int64_t> rand_dist(std::numeric_limits<int64_t>::min());

        int64_t when_in_millis = when_in_micros / 1000;
        int64_t nanos = (when_in_micros - (when_in_millis * 1000)) * 10;

        auto uuid = UUID(create_time(from_unix_timestamp(when_in_millis) + nanos), rand_dist(rand_gen));
        assert(uuid.is_timestamp());
        return uuid;
    }
    // Generate a time-based (Version 1) UUID using
    // a microsecond-precision Unix time and a unique number in
    // range [0, 131072).
    // Used to generate many unique, monotonic UUIDs
    // sharing the same microsecond part. In lightweight
    // transactions we must ensure monotonicity between all UUIDs
    // which belong to one lightweight transaction and UUIDs of
    // another transaction, but still need multiple distinct and
    // monotonic UUIDs within the same transaction.
    // \throws timeuuid_submicro_out_of_range
    //
    static std::array<int8_t, 16>
    get_time_UUID_bytes_from_micros_and_submicros(int64_t when_in_micros, int submicros) {
        std::array<int8_t, 16> uuid_bytes;

        if (submicros < 0 || submicros >= SUBMICRO_LIMIT) {
            throw timeuuid_submicro_out_of_range("timeuuid submicro component does not fit into available bits");
        }

        auto dmc = from_unix_timestamp(std::chrono::microseconds(when_in_micros));
        // We have roughly 3 extra bits we will use to increase
        // sub-microsecond component range from clockseq's 2^14 to 2^17.
        int64_t msb = create_time(dmc + decimicroseconds((submicros >> 14) & 0b111));
        // See RFC 4122 for details.
        msb = net::hton(msb);

        std::copy_n(reinterpret_cast<char*>(&msb), sizeof(msb), uuid_bytes.data());

        // Use 14-bit clockseq to store the rest of sub-microsecond component.
        int64_t clockseq = submicros & 0b11'1111'1111'1111;
        // Scylla, like Cassandra, uses signed int8 compare to
        // compare lower bits of timeuuid. It means 0xA0 > 0xFF.
        // Bit-xor the sign bit to "fix" the order. See also
        // https://issues.apache.org/jira/browse/CASSANDRA-8730
        // and Cassandra commit 6d266253a5bdaf3a25eef14e54deb56aba9b2944
        //
        // Turn 0 into -127, 1 into -126, ... and 128 into 0, ...
        clockseq ^=  0b0000'0000'1000'0000;
        // Least significant bits: UUID variant (1), clockseq and node.
        // To protect against the system clock back-adjustment,
        // use a random (spoof) node identifier. Normally this
        // protection is provided by clockseq component, but we've
        // just stored sub-microsecond time in it.
        int64_t lsb = ((clockseq | 0b1000'0000'0000'0000) << 48) | UUID_gen::spoof_node;
        lsb = net::hton(lsb);

        std::copy_n(reinterpret_cast<char*>(&lsb), sizeof(lsb), uuid_bytes.data() + sizeof(msb));

        return uuid_bytes;
    }

    /** validates uuid from raw bytes. */
    static bool is_valid_UUID(bytes raw) {
        return raw.size() == 16;
    }

    /** creates uuid from raw bytes. */
    static UUID get_UUID(bytes raw) {
        assert(raw.size() == 16);
        return get_UUID(raw.begin());
    }

    /** creates uuid from raw bytes. src must point to a region of 16 bytes*/
    static UUID get_UUID(int8_t* src) {
        struct tmp { uint64_t msb, lsb; } t;
        std::copy(src, src + 16, reinterpret_cast<char*>(&t));
        return UUID(net::ntoh(t.msb), net::ntoh(t.lsb));
    }

    /**
     * Creates a type 3 (name based) UUID based on the specified byte array.
     */
    static UUID get_name_UUID(bytes_view b);
    static UUID get_name_UUID(sstring_view str);
    static UUID get_name_UUID(const unsigned char* s, size_t len);

    /** decomposes a uuid into raw bytes. */
    static std::array<int8_t, 16> decompose(const UUID& uuid)
    {
        uint64_t most = uuid.get_most_significant_bits();
        uint64_t least = uuid.get_least_significant_bits();
        std::array<int8_t, 16> b;
        for (int i = 0; i < 8; i++)
        {
            b[i] = (char)(most >> ((7-i) * 8));
            b[8+i] = (char)(least >> ((7-i) * 8));
        }
        return b;
    }

    /**
     * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
     * based on the current system time.
     *
     * @return a type 1 UUID represented as a byte[]
     */
    static std::array<int8_t, 16> get_time_UUID_bytes()
    {
        return create_time_UUID_bytes(instance->create_time_safe());
    }

    /**
     * Returns the smaller possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    static UUID min_time_UUID(int64_t timestamp)
    {
        auto uuid = UUID(create_time(from_unix_timestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
        assert(uuid.is_timestamp());
        return uuid;
    }

    /**
     * Returns the biggest possible type 1 UUID having the provided timestamp.
     *
     * <b>Warning:</b> this method should only be used for querying as this
     * doesn't at all guarantee the uniqueness of the resulting UUID.
     */
    static UUID max_time_UUID(int64_t timestamp)
    {
        // unix timestamp are milliseconds precision, uuid timestamp are 100's
        // nanoseconds precision. If we ask for the biggest uuid have unix
        // timestamp 1ms, then we should not extend 100's nanoseconds
        // precision by taking 10000, but rather 19999.
        int64_t uuid_tstamp = from_unix_timestamp(timestamp + 1) - 1;
        auto uuid = UUID(create_time(uuid_tstamp), MAX_CLOCK_SEQ_AND_NODE);
        assert(uuid.is_timestamp());
        return uuid;
    }

    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    static int64_t unix_timestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param uuid
     * @return seconds since Unix epoch
     */
    static std::chrono::seconds unix_timestamp_in_sec(UUID uuid)
    {
        using namespace std::chrono;
        return duration_cast<seconds>(static_cast<milliseconds>(unix_timestamp(uuid)));
    }

    /**
     * @param uuid
     * @return microseconds since Unix epoch
     */
    static int64_t micros_timestamp(UUID uuid)
    {
        return (uuid.timestamp() / 10) + START_EPOCH * 1000;
    }

private:
    template <std::intmax_t N, std::intmax_t D>
    static decimicroseconds from_unix_timestamp(std::chrono::duration<int64_t, std::ratio<N, D>> d) {
        return d - std::chrono::milliseconds(START_EPOCH);
    }
    /**
     * @param timestamp milliseconds since Unix epoch
     * @return
     */
    static int64_t from_unix_timestamp(int64_t timestamp) {
        return (timestamp - START_EPOCH) * 10000;
    }

public:
    /**
     * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     *
     * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
     * a number 0 <= n < 10000 such that n*100 is the number of nanoseconds within that millisecond.
     *
     * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
     * invocations using identical timestamps will result in identical UUIDs.</i></p>
     *
     * @return a type 1 UUID represented as a byte[]
     */
    static std::array<int8_t, 16> get_time_UUID_bytes(int64_t time_millis, int nanos)
    {
#if 0
        if (nanos >= 10000)
            throw new IllegalArgumentException();
#endif
        return create_time_UUID_bytes(instance->create_time_unsafe(time_millis, nanos));
    }

private:
    static std::array<int8_t, 16> create_time_UUID_bytes(uint64_t msb)
    {
        uint64_t lsb = clock_seq_and_node;
        std::array<int8_t, 16> uuid_bytes;

        for (int i = 0; i < 8; i++)
            uuid_bytes[i] = (int8_t) (msb >> 8 * (7 - i));

        for (int i = 8; i < 16; i++)
            uuid_bytes[i] = (int8_t) (lsb >> 8 * (7 - (i - 8)));

        return uuid_bytes;
    }

public:
    /**
     * Returns a milliseconds-since-epoch value for a type-1 UUID.
     *
     * @param uuid a type-1 (time-based) UUID
     * @return the number of milliseconds since the unix epoch
     * @throws IllegalArgumentException if the UUID is not version 1
     */
    static int64_t get_adjusted_timestamp(UUID uuid)
    {
#if 0
        if (uuid.version() != 1)
            throw new IllegalArgumentException("incompatible with uuid version: "+uuid.version());
#endif
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    static uint64_t make_nanos_since(int64_t millis) {
        return (static_cast<uint64_t>(millis) - static_cast<uint64_t>(START_EPOCH)) * 10000;
    }

    // nanos_since must fit in 60 bits
    static bool is_valid_nanos_since(uint64_t nanos_since) {
        return !(0xf000000000000000UL & nanos_since);
    }

private:

    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    // NOTE: In the original Java code this function was "synchronized". This isn't
    // needed if we assume our code will run on just one CPU.
    int64_t create_time_safe()
    {
        using namespace std::chrono;
        int64_t millis = duration_cast<milliseconds>(
                system_clock::now().time_since_epoch()).count();
        uint64_t nanos_since = make_nanos_since(millis);
        if (nanos_since > last_nanos)
            last_nanos = nanos_since;
        else
            nanos_since = ++last_nanos;

        return create_time(nanos_since);
    }

    int64_t create_time_unsafe(int64_t when, int nanos)
    {
        uint64_t nanos_since = make_nanos_since(when) + static_cast<uint64_t>(static_cast<int64_t>(nanos));
        return create_time(nanos_since);
    }

    // std::chrono typeaware wrapper around create_time().
    // Creates a timeuuid compatible time (decimicroseconds since
    // the start of GMT epoch).
    template <std::intmax_t N, std::intmax_t D>
    static int64_t create_time(std::chrono::duration<int64_t, std::ratio<N, D>> d) {
        return create_time(duration_cast<decimicroseconds>(d).count());
    }

public:

    static int64_t create_time(uint64_t nanos_since)
    {
        uint64_t msb = 0L;
        assert(is_valid_nanos_since(nanos_since));
        msb |= (0x00000000ffffffffL & nanos_since) << 32;
        msb |= (0x0000ffff00000000UL & nanos_since) >> 16;
        msb |= (0x0fff000000000000UL & nanos_since) >> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }
};

// for the curious, here is how I generated START_EPOCH
//        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
//        c.set(Calendar.YEAR, 1582);
//        c.set(Calendar.MONTH, Calendar.OCTOBER);
//        c.set(Calendar.DAY_OF_MONTH, 15);
//        c.set(Calendar.HOUR_OF_DAY, 0);
//        c.set(Calendar.MINUTE, 0);
//        c.set(Calendar.SECOND, 0);
//        c.set(Calendar.MILLISECOND, 0);
//        long START_EPOCH = c.getTimeInMillis();

} // namespace utils
