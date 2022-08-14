/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "idl/mutation.idl.hh"
#include "idl/frozen_mutation.idl.hh"

class commitlog_entry [[writable]] {
    std::optional<column_mapping> mapping();
    frozen_mutation mutation();
};
