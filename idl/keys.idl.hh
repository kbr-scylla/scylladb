/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class clustering_key_prefix {
    std::vector<bytes> explode();
};

class partition_key {
    std::vector<bytes> explode();
};
