/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

namespace streaming {

class stream_request {
    sstring keyspace;
    // For compatibility with <= 1.5, we use wrapping ranges
    // (though we never send wraparounds; only allow receiving them)
    std::vector<range<dht::token>> ranges_compat();
    std::vector<sstring> column_families;
};

class stream_summary {
    utils::UUID cf_id;
    int files;
    long total_size;
};


class prepare_message {
    std::vector<streaming::stream_request> requests;
    std::vector<streaming::stream_summary> summaries;
    uint32_t dst_cpu_id;
};

enum class stream_reason : uint8_t {
    unspecified,
    bootstrap,
    decommission,
    removenode,
    rebuild,
    repair,
    replace,
};

enum class stream_mutation_fragments_cmd : uint8_t {
    error,
    mutation_fragment_data,
    end_of_stream,
};

}
