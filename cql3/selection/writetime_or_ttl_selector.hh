/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (ScyllaDB-Proprietary and Apache-2.0)
 */

#pragma once

#include "selector.hh"
#include "selection.hh"

namespace cql3 {

namespace selection {

class writetime_or_ttl_selector : public selector {
    sstring _column_name;
    int _idx;
    bool _is_writetime;
    bytes_opt _current;
public:
    static shared_ptr<selector::factory> new_factory(sstring column_name, int idx, bool is_writetime) {
        class wtots_factory : public selector::factory {
            sstring _column_name;
            int _idx;
            bool _is_writetime;
        public:
            wtots_factory(sstring column_name, int idx, bool is_writetime)
                : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
            }

            virtual sstring column_name() const override {
                return format("{}({})", _is_writetime ? "writetime" : "ttl", _column_name);
            }

            virtual data_type get_return_type() const override {
                return _is_writetime ? long_type : int32_type;
            }

            virtual shared_ptr<selector> new_instance() const override {
                return ::make_shared<writetime_or_ttl_selector>(_column_name, _idx, _is_writetime);
            }

            virtual bool is_write_time_selector_factory() const override {
                return _is_writetime;
            }

            virtual bool is_ttl_selector_factory() const override {
                return !_is_writetime;
            }
        };
        return ::make_shared<wtots_factory>(std::move(column_name), idx, is_writetime);
    }

    virtual void add_input(cql_serialization_format sf, result_set_builder& rs) override {
        if (_is_writetime) {
            int64_t ts = rs.timestamp_of(_idx);
            if (ts != api::missing_timestamp) {
                _current = bytes(bytes::initialized_later(), 8);
                auto i = _current->begin();
                serialize_int64(i, ts);
            } else {
                _current = std::nullopt;
            }
        } else {
            int ttl = rs.ttl_of(_idx);
            if (ttl > 0) {
                _current = bytes(bytes::initialized_later(), 4);
                auto i = _current->begin();
                serialize_int32(i, ttl);
            } else {
                _current = std::nullopt;
            }
        }
    }

    virtual bytes_opt get_output(cql_serialization_format sf) override {
        return _current;
    }

    virtual void reset() override {
        _current = std::nullopt;
    }

    virtual data_type get_type() const override {
        return _is_writetime ? long_type : int32_type;
    }

    virtual sstring assignment_testable_source_context() const override {
        return _column_name;
    }

#if 0
    @Override
    public String toString()
    {
        return columnName;
    }
#endif

    writetime_or_ttl_selector(sstring column_name, int idx, bool is_writetime)
            : _column_name(std::move(column_name)), _idx(idx), _is_writetime(is_writetime) {
    }
};

}
}
