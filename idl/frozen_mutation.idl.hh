/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

class frozen_mutation final {
    bytes representation();
};

class frozen_mutation_fragment final {
    bytes representation();
};
