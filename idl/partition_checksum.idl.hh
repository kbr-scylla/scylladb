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

class repair_hash {
    uint64_t hash;
};

enum class bound_weight : int8_t {
    before_all_prefixed = -1,
    equal = 0,
    after_all_prefixed = 1,
};

enum class partition_region : uint8_t {
    partition_start,
    static_row,
    clustered,
    partition_end,
};

class position_in_partition {
    partition_region get_type();
    bound_weight get_bound_weight();
    std::optional<clustering_key_prefix> get_clustering_key_prefix();
};

struct partition_key_and_mutation_fragments {
    partition_key get_key();
    std::list<frozen_mutation_fragment> get_mutation_fragments();
};

class repair_sync_boundary {
    dht::decorated_key pk;
    position_in_partition position;
};

struct get_sync_boundary_response {
    std::optional<repair_sync_boundary> boundary;
    repair_hash row_buf_combined_csum;
    uint64_t row_buf_size;
    uint64_t new_rows_size;
    uint64_t new_rows_nr;
};

struct get_combined_row_hash_response {
    repair_hash working_row_buf_combined_csum;
    uint64_t working_row_buf_nr;
};

enum class row_level_diff_detect_algorithm : uint8_t {
    send_full_set,
};
