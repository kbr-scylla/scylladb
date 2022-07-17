/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "expression.hh"

namespace cql3 {
namespace expr {

// Given a restriction from the WHERE clause prepares it and performs some validation checks.
// It will also fill the prepare context automatically, there's no need to do that later.
binary_operator validate_and_prepare_new_restriction(const binary_operator& restriction,
                                                     data_dictionary::database db,
                                                     schema_ptr schema,
                                                     prepare_context& ctx);


// Converts a prepared binary operator to an instance of the restriction class.
// Doesn't perform any any validation checks.
::shared_ptr<restrictions::restriction> convert_to_restriction(const binary_operator& prepared_binop,
                                                               const schema_ptr& schema);

} // namespace expr
} // namespace cql3
