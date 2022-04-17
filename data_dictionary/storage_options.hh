/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <string>
#include <map>
#include <seastar/core/sstring.hh>

namespace data_dictionary {

struct storage_options {
    struct local {
        friend auto operator<=>(const local&, const local&) = default;
    };
    struct s3 {
        sstring bucket;
        sstring endpoint;

        friend auto operator<=>(const s3&, const s3&) = default;
    };
    using value_type = std::variant<local, s3>;
    value_type value = local{};

    storage_options() = default;

    std::string_view type_string() const;
    std::map<sstring, sstring> to_map() const;

    bool can_update_to(const storage_options& new_options);

    static value_type from_map(std::string_view type, std::map<sstring, sstring> values);
};

} // namespace data_dictionary
