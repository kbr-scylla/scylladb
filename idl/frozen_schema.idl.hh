/*
 * Copyright 2016-present ScyllaDB
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
    std::optional<canonical_mutation> indices_canonical_mutation()[[version 2.0]];
    std::optional<canonical_mutation> dropped_columns_canonical_mutation()[[version 2.0]];
    std::optional<canonical_mutation> scylla_tables_canonical_mutation()[[version 2.0]];
    std::optional<canonical_mutation> view_virtual_columns_canonical_mutation()[[version 2.4]];
    std::optional<canonical_mutation> computed_columns_canonical_mutation()[[version 3.2]];
};

class schema stub [[writable]] {
    utils::UUID version;
    schema_mutations mutations;
};

class frozen_schema final {
    bytes representation();
};
