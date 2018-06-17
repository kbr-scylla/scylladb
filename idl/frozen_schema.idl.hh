/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

class canonical_mutation final {
    bytes representation();
};

class schema_mutations {
    canonical_mutation columnfamilies_canonical_mutation();
    canonical_mutation columns_canonical_mutation();
    bool is_view()[[version 1.6]];
    std::experimental::optional<canonical_mutation> indices_canonical_mutation()[[version 2.0]];
    std::experimental::optional<canonical_mutation> dropped_columns_canonical_mutation()[[version 2.0]];
    std::experimental::optional<canonical_mutation> scylla_tables_canonical_mutation()[[version 2.0]];
};

class schema stub [[writable]] {
    utils::UUID version;
    schema_mutations mutations;
};

class frozen_schema final {
    bytes representation();
};
