/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "serializer.hh"
#include "schema.hh"
#include "db/extensions.hh"

namespace alternator {

class tags_extension : public schema_extension {
public:
    static constexpr auto NAME = "scylla_tags";

    tags_extension() = default;
    explicit tags_extension(const std::map<sstring, sstring>& tags) : _tags(std::move(tags)) {}
    explicit tags_extension(bytes b) : _tags(tags_extension::deserialize(b)) {}
    explicit tags_extension(const sstring& s) {
        throw std::logic_error("Cannot create tags from string");
    }
    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_tags);
    }
    static std::map<sstring, sstring> deserialize(bytes_view buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<std::map<sstring, sstring>>());
    }
    const std::map<sstring, sstring>& tags() const {
        return _tags;
    }
private:
    std::map<sstring, sstring> _tags;
};

}
