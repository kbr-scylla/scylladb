/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

enum class repair_checksum : uint8_t {
    legacy = 0,
    streamed = 1,
};

class partition_checksum {
  std::array<uint8_t, 32> digest();
};
