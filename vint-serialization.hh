/*
 * Copyright 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

//
// For reference, see https://developers.google.com/protocol-buffers/docs/encoding#varints
//

#include "bytes.hh"

#include <cstdint>

using vint_size_type = bytes::size_type;

struct unsigned_vint final {
    using value_type = uint64_t;

    struct deserialized_type final {
        value_type value;
        vint_size_type size;
    };

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static deserialized_type deserialize(bytes_view v);
};

struct signed_vint final {
    using value_type = int64_t;

    struct deserialized_type final {
        value_type value;
        vint_size_type size;
    };

    static vint_size_type serialized_size(value_type) noexcept;

    static vint_size_type serialize(value_type, bytes::iterator out);

    static deserialized_type deserialize(bytes_view v);
};
