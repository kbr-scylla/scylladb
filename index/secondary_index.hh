/*
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

namespace db {
namespace index {

/**
 * Abstract base class for different types of secondary indexes.
 *
 * Do not extend this directly, please pick from PerColumnSecondaryIndex or PerRowSecondaryIndex
 */
class secondary_index {
public:
    static const sstring custom_index_option_name;

    /**
     * The name of the option used to specify that the index is on the collection keys.
     */
    static const sstring index_keys_option_name;

    /**
     * The name of the option used to specify that the index is on the collection values.
     */
    static const sstring index_values_option_name;

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    static const sstring index_entries_option_name;

};

}
}
