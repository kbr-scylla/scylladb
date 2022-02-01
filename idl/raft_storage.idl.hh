/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

namespace raft {

namespace internal {

template<typename Tag>
struct tagged_id {
    utils::UUID id;
};

template<typename Tag>
struct tagged_uint64 {
    uint64_t get_value();
};

} // namespace internal

struct server_address {
    raft::server_id id;
    bool can_vote;
    bytes info;
};

struct configuration {
    std::unordered_set<raft::server_address> current;
    std::unordered_set<raft::server_address> previous;
};

struct log_entry {
    struct dummy {};

    raft::term_t term;
    raft::index_t idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

}
