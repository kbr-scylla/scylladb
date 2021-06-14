/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
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

struct server_id_tag stub {};
struct snapshot_id_tag stub {};
struct index_tag stub {};
struct term_tag stub {};

struct server_address {
    raft::internal::tagged_id<raft::server_id_tag> id;
    bool can_vote;
    bytes info;
};

struct configuration {
    std::unordered_set<raft::server_address> current;
    std::unordered_set<raft::server_address> previous;
};

struct snapshot {
    raft::internal::tagged_uint64<raft::index_tag> idx;
    raft::internal::tagged_uint64<raft::term_tag> term;
    raft::configuration config;
    raft::internal::tagged_id<raft::snapshot_id_tag> id;
};

struct vote_request {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_uint64<raft::index_tag> last_log_idx;
    raft::internal::tagged_uint64<raft::term_tag> last_log_term;
    bool is_prevote;
    bool force;
};

struct vote_reply {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    bool vote_granted;
    bool is_prevote;
};

struct install_snapshot {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::snapshot snp;
};

struct snapshot_reply {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    bool success;
};

struct append_reply {
    struct rejected {
        raft::internal::tagged_uint64<raft::index_tag> non_matching_idx;
        raft::internal::tagged_uint64<raft::index_tag> last_idx;
    };
    struct accepted {
        raft::internal::tagged_uint64<raft::index_tag> last_new_idx;
    };
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_uint64<raft::index_tag> commit_idx;
    std::variant<raft::append_reply::rejected, raft::append_reply::accepted> result;
};

struct log_entry {
    struct dummy {};

    raft::internal::tagged_uint64<raft::term_tag> term;
    raft::internal::tagged_uint64<raft::index_tag> idx;
    std::variant<bytes_ostream, raft::configuration, raft::log_entry::dummy> data;
};

struct append_request {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
    raft::internal::tagged_id<raft::server_id_tag> leader_id;
    raft::internal::tagged_uint64<raft::index_tag> prev_log_idx;
    raft::internal::tagged_uint64<raft::term_tag> prev_log_term;
    raft::internal::tagged_uint64<raft::index_tag> leader_commit_idx;
    std::vector<lw_shared_ptr<const raft::log_entry>> entries;
};

struct timeout_now {
    raft::internal::tagged_uint64<raft::term_tag> current_term;
};

} // namespace raft
