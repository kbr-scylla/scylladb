/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

// database.hh
class database;
class column_family;
class memtable_list;

// mutation.hh
class mutation;
class mutation_partition;

// schema.hh
class schema;
class column_definition;
class column_mapping;

// schema_mutations.hh
class schema_mutations;

// keys.hh
class exploded_clustering_prefix;
class partition_key;
class partition_key_view;
class clustering_key_prefix;
class clustering_key_prefix_view;
using clustering_key = clustering_key_prefix;
using clustering_key_view = clustering_key_prefix_view;

// memtable.hh
class memtable;
