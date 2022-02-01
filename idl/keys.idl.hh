/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

class clustering_key_prefix {
    std::vector<bytes> explode();
};

class partition_key {
    std::vector<bytes> explode();
};
