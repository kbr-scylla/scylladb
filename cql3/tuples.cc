/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <seastar/core/shared_ptr.hh>

#include "tuples.hh"
#include "types/list.hh"

namespace cql3 {

tuples::in_value
tuples::in_value::from_serialized(const raw_value_view& value_view, const list_type_impl& type, const query_options& options) {
    try {
        // Collections have this small hack that validate cannot be called on a serialized object,
        // but the deserialization does the validation (so we're fine).
        auto l = value_view.deserialize<list_type_impl::native_type>(type, options.get_cql_serialization_format());
        auto ttype = dynamic_pointer_cast<const tuple_type_impl>(type.get_elements_type());
        assert(ttype);

        utils::chunked_vector<std::vector<managed_bytes_opt>> elements;
        elements.reserve(l.size());
        for (auto&& e : l) {
            // FIXME: Avoid useless copies.
            elements.emplace_back(ttype->split_fragmented(single_fragmented_view(ttype->decompose(e))));
        }
        return tuples::in_value(elements);
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

tuples::in_marker::in_marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
    : abstract_marker(bind_index, std::move(receiver))
{
    assert(dynamic_pointer_cast<const list_type_impl>(_receiver->type));
}

shared_ptr<terminal> tuples::in_marker::bind(const query_options& options) {
    const auto& value = options.get_value_at(_bind_index);
    if (value.is_null()) {
        return nullptr;
    } else if (value.is_unset_value()) {
        throw exceptions::invalid_request_exception(format("Invalid unset value for tuple {}", _receiver->name->text()));
    } else {
        auto& type = static_cast<const list_type_impl&>(*_receiver->type);
        auto& elem_type = static_cast<const tuple_type_impl&>(*type.get_elements_type());
        try {
            auto l = value.validate_and_deserialize<list_type_impl::native_type>(type, options.get_cql_serialization_format());
            for (auto&& element : l) {
                elem_type.validate(elem_type.decompose(element), options.get_cql_serialization_format());
            }
        } catch (marshal_exception& e) {
            throw exceptions::invalid_request_exception(e.what());
        }
        return make_shared<tuples::in_value>(tuples::in_value::from_serialized(value, type, options));
    }
}

}
