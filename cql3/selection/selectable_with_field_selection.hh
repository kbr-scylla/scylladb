/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */


#pragma once

#include "selectable.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

namespace selection {

class selectable::with_field_selection : public selectable {
public:
    shared_ptr<selectable> _selected;
    shared_ptr<column_identifier> _field;
public:
    with_field_selection(shared_ptr<selectable> selected, shared_ptr<column_identifier> field)
            : _selected(std::move(selected)), _field(std::move(field)) {
    }

    virtual sstring to_string() const override;

    virtual shared_ptr<selector::factory> new_selector_factory(data_dictionary::database db, schema_ptr s, std::vector<const column_definition*>& defs) override;
};

}

}
