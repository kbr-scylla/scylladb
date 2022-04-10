/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include <filesystem>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "schema.hh"

namespace tools {

/// Load the schema(s) from the specified string
///
/// The schema string is expected to contain everything that is needed to
/// create the table(s): keyspace, UDTs, etc. Definitions are expected to be
/// separated by `;`. A keyspace will be automatically generated if missing.
/// Loading the schema(s) has no side-effect [1]. Nothing is written to disk,
/// it is all in memory, kept alive by the returned `schema_ptr`.
/// This is intended to be used by tools, which don't want to meddle with the
/// scylla home directory.
///
/// [1] Currently some global services has to be instantiated (snitch) to
/// be able to load the schema(s), these survive the call.
future<std::vector<schema_ptr>> load_schemas(std::string_view schema_str);

/// Load exactly one schema from the specified string
///
/// If the string contains more or less then one schema, an exception will be
/// thrown. See \ref load_schemas().
future<schema_ptr> load_one_schema(std::string_view schema_str);

/// Load the schema(s) from the specified path
///
/// Same as \ref load_schemas() except it loads the schema from
/// the file at the specified path.
future<std::vector<schema_ptr>> load_schemas_from_file(std::filesystem::path path);

/// Load exactly one schema from the specified path
///
/// Same as \ref load_one_schema() except it loads the schema from
/// the file at the specified path.
future<schema_ptr> load_one_schema_from_file(std::filesystem::path path);

/// Load the system schema, with the given keyspace and table
///
/// Note that only schemas from builtin system tables are supported, i.e.,
/// from the following keyspaces:
/// * system
/// * system_schema
/// * system_distributed
/// * system_distributed_everywhere
///
/// Any table from said keyspaces can be loaded. The keyspaces are created with
/// all schema and experimental features enabled.
schema_ptr load_system_schema(std::string_view keyspace, std::string_view table);

} // namespace tools
