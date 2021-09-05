/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"

namespace mutation_writer {

class bucket_writer {
    schema_ptr _schema;
    queue_reader_handle _handle;
    future<> _consume_fut;

private:
    bucket_writer(schema_ptr schema, std::pair<flat_mutation_reader, queue_reader_handle> queue_reader, reader_consumer& consumer);

public:
    bucket_writer(schema_ptr schema, reader_permit permit, reader_consumer& consumer);

    future<> consume(mutation_fragment mf);

    void consume_end_of_stream();

    void abort(std::exception_ptr ep) noexcept;

    future<> close() noexcept;
};

template <typename Writer>
requires MutationFragmentConsumer<Writer, future<>>
future<> feed_writer(flat_mutation_reader&& rd, Writer&& wr) {
    return do_with(std::move(rd), std::move(wr), [] (flat_mutation_reader& rd, Writer& wr) {
        return rd.fill_buffer().then([&rd, &wr] {
            return do_until([&rd] { return rd.is_buffer_empty() && rd.is_end_of_stream(); }, [&rd, &wr] {
                auto f1 = rd.pop_mutation_fragment().consume(wr);
                auto f2 = rd.is_buffer_empty() ? rd.fill_buffer() : make_ready_future<>();
                return when_all_succeed(std::move(f1), std::move(f2)).discard_result();
            });
        }).then_wrapped([&wr] (future<> f) {
            if (f.failed()) {
                auto ex = f.get_exception();
                wr.abort(ex);
                return wr.close().then_wrapped([ex = std::move(ex)] (future<> f) mutable {
                    if (f.failed()) {
                        // The consumer is expected to fail when aborted,
                        // so just ignore any exception.
                        (void)f.get_exception();
                    }
                    return make_exception_future<>(std::move(ex));
                });
            } else {
                wr.consume_end_of_stream();
                return wr.close();
            }
        }).finally([&rd] {
            return rd.close();
        });
    });
}

}
