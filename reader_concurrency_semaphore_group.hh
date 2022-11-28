/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

/*
 * Copyright (C) 2021-present ScyllaDB
 */

#pragma once

#include <unordered_map>
#include "reader_concurrency_semaphore.hh"
#include <boost/range/adaptor/map.hpp>

// The reader_concurrency_semaphore_group is a group of semaphores that shares a common pool of memory,
// the memory is dynamically divided between them according to a relative slice of shares each semaphore
// is given.
// All of the mutating operations on the group are asynchronic and serialized. The semaphores are created
// and managed by the group.

class reader_concurrency_semaphore_group {
    size_t _total_memory;
    size_t _total_weight;
    size_t _max_concurrent_reads;
    size_t _max_queue_length;
    friend class database_test;

    struct weighted_reader_concurrency_semaphore {
        size_t weight;
        ssize_t memory_share;
        reader_concurrency_semaphore sem;
        weighted_reader_concurrency_semaphore(size_t shares, int count, sstring name, size_t max_queue_length)
                : weight(shares)
                , memory_share(0)
                , sem(count, 0, name, max_queue_length) {}
    };

    std::unordered_map<scheduling_group, weighted_reader_concurrency_semaphore> _semaphores;
    seastar::semaphore _operations_serializer;

    future<> change_weight(weighted_reader_concurrency_semaphore& sem, size_t new_weight);

public:
    reader_concurrency_semaphore_group(size_t memory, size_t max_concurrent_reads, size_t max_queue_length)
            : _total_memory(memory)
            , _total_weight(0)
            , _max_concurrent_reads(max_concurrent_reads)
            ,  _max_queue_length(max_queue_length)
            , _operations_serializer(1) { }

    ~reader_concurrency_semaphore_group() {
        assert(_semaphores.empty());
    }
    future<> adjust();
    future<> wait_adjust_complete();

    future<> stop() noexcept;
    reader_concurrency_semaphore& get(scheduling_group sg);
    reader_concurrency_semaphore* get_or_null(scheduling_group sg);
    reader_concurrency_semaphore& add_or_update(scheduling_group sg, size_t shares);
    future<> remove(scheduling_group sg);
    size_t size();
    void foreach_semaphore(std::function<void(scheduling_group, reader_concurrency_semaphore&)> func);

    future<> foreach_semaphore_async(std::function<future<> (scheduling_group, reader_concurrency_semaphore&)> func);

    auto sum_read_concurrency_sem_var(std::invocable<reader_concurrency_semaphore&> auto member) {
        using ret_type = std::invoke_result_t<decltype(member), reader_concurrency_semaphore&>;
        return boost::accumulate(_semaphores | boost::adaptors::map_values | boost::adaptors::transformed([=] (weighted_reader_concurrency_semaphore& wrcs) { return std::invoke(member, wrcs.sem); }), ret_type(0));
    }
};
