/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class frozen_mutation final {
    bytes representation();
};

class frozen_mutation_fragment final {
    bytes representation();
};
