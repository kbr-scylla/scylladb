/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/shared_future.hh>

#include "flat_mutation_reader_v2.hh"

// A reader which allows to insert a deferring operation before reading.
// All calls will first wait for a future to resolve, then forward to a given underlying reader.
class chained_delegating_reader : public flat_mutation_reader_v2::impl {
    std::unique_ptr<flat_mutation_reader_v2> _underlying;
    std::function<future<flat_mutation_reader_v2>()> _populate_reader;
    std::function<void()> _on_destroyed;
    
public:
    chained_delegating_reader(schema_ptr s, std::function<future<flat_mutation_reader_v2>()>&& populate, reader_permit permit, std::function<void()> on_destroyed = []{})
        : impl(s, std::move(permit))
        , _populate_reader(std::move(populate))
        , _on_destroyed(std::move(on_destroyed))
    { }

    chained_delegating_reader(chained_delegating_reader&& rd) = delete;

    ~chained_delegating_reader() {
        _on_destroyed();
    }

    virtual future<> fill_buffer() override {
        if (!_underlying) {
            return _populate_reader().then([this] (flat_mutation_reader_v2&& rd) {
                _underlying = std::make_unique<flat_mutation_reader_v2>(std::move(rd));
                return fill_buffer();
            });
        }

        if (is_buffer_full()) {
            return make_ready_future<>();
        }

        return _underlying->fill_buffer().then([this] {
            _end_of_stream = _underlying->is_end_of_stream();
            _underlying->move_buffer_content_to(*this);
        });
    }

    virtual future<> fast_forward_to(position_range pr) override {
        if (!_underlying) {
            return _populate_reader().then([this, pr = std::move(pr)] (flat_mutation_reader_v2&& rd) mutable {
                _underlying = std::make_unique<flat_mutation_reader_v2>(std::move(rd));
                return fast_forward_to(pr);
            });
        }

        _end_of_stream = false;
        forward_buffer_to(pr.start());
        return _underlying->fast_forward_to(std::move(pr));
    }

    virtual future<> next_partition() override {
        if (!_underlying) {
            return make_ready_future<>();
        }

        clear_buffer_to_next_partition();
        auto f = make_ready_future<>();
        if (is_buffer_empty()) {
            f = _underlying->next_partition();
        }
        _end_of_stream = _underlying->is_end_of_stream() && _underlying->is_buffer_empty();

        return f;
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        if (!_underlying) {
            return _populate_reader().then([this, &pr] (flat_mutation_reader_v2&& rd) mutable {
                _underlying = std::make_unique<flat_mutation_reader_v2>(std::move(rd));
                return fast_forward_to(pr);
            });
        }

        _end_of_stream = false;
        clear_buffer();
        return _underlying->fast_forward_to(pr);
    }

    virtual future<> close() noexcept override {
        if (_underlying) {
            return _underlying->close();
        }
        return make_ready_future<>();
    }
};
