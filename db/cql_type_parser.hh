/*
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include <vector>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>

#include "types.hh"

#include "seastarx.hh"

class types_metadata;

namespace data_dictionary {
class keyspace_metadata;
}

namespace db {
namespace cql_type_parser {

data_type parse(const sstring& keyspace, const sstring& type);

class raw_builder {
public:
    raw_builder(data_dictionary::keyspace_metadata &ks);
    ~raw_builder();

    void add(sstring name, std::vector<sstring> field_names, std::vector<sstring> field_types);
    std::vector<user_type> build();
private:
    class impl;
    std::unique_ptr<impl>
        _impl;
};

}
}
