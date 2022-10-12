/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#include "reader_concurrency_semaphore_group.hh"

ssize_t reader_concurrency_semaphore_group::calc_delta(weighted_reader_concurrency_semaphore& sem) const {
    auto new_memory_share = _total_weight ? (_total_memory * sem.weight) / _total_weight : 0;
    return new_memory_share - sem.memory_share;
}

void reader_concurrency_semaphore_group::distribute_spare_memory(semaphore_priority_type& memory_short_semaphores) {
    while (!memory_short_semaphores.empty() && _spare_memory > 0) {
        auto&& memory_short_sem = *(memory_short_semaphores.top());
        memory_short_semaphores.pop();
        adjust_up(memory_short_sem);
        if (calc_delta(memory_short_sem) > 0 ) {
            memory_short_semaphores.push(&memory_short_sem);
        }
    }
}

future<> reader_concurrency_semaphore_group::reduce_memory(weighted_reader_concurrency_semaphore& sem, size_t reduction_amount) {
    return sem.sem.with_permit(nullptr, "adjust memory down", reduction_amount, db::timeout_clock::time_point::max(), [this, reduction_amount, &sem] (reader_permit permit) noexcept {
            sem.sem.consume({0, reduction_amount});
            sem.memory_share -= reduction_amount;
            return make_ready_future();
    });
}

// for decreasing operation we first need to make sure we have a memory to decrease.
// the easiest way (or at least most standard way) is to first consume it.
future<> reader_concurrency_semaphore_group::adjust_down(weighted_reader_concurrency_semaphore& sem) {
    ssize_t delta = calc_delta(sem);
    if ( delta < 0 ) {
        return reduce_memory(sem, std::abs(delta)).then([this, &sem, reduction_amount = std::abs(delta)] () {
            _spare_memory += reduction_amount;
        });
    }
    return make_ready_future();
}


void reader_concurrency_semaphore_group::adjust_up(weighted_reader_concurrency_semaphore& sem) {
    ssize_t delta = calc_delta(sem);
    // if we are increasing memory - delta will be positive.
    if (( delta > 0 ) && (_spare_memory > 0)) {
        auto mem_amount = std::min<size_t>(_spare_memory, std::abs(delta));
        sem.sem.signal({0, mem_amount});
        auto old_memory_share = sem.memory_share;
        auto old_spare_memory = _spare_memory;
        sem.memory_share += mem_amount;
        _spare_memory -= mem_amount;
    }
}

// Calling adjust is serialized since 2 adjustments can't happen simultaneosly,
// if they did the behaviour would be undefined.
future<> reader_concurrency_semaphore_group::adjust() {
    return with_semaphore(_operations_serializer, 1, [this] () {
        ssize_t distributed_memory = 0;
        for (auto& [sg, wsem] : _semaphores) {
            const ssize_t memory_share = std::floor((double(wsem.weight) / double(_total_weight)) * _total_memory);
            wsem.sem.set_resources({_max_concurrent_reads, memory_share});
            distributed_memory += memory_share;
        }
        // Slap the remainder on one of the semaphores.
        // This will be a few bytes, doesn't matter where we add it.
        auto& sem = _semaphores.begin()->second.sem;
        sem.set_resources(sem.initial_resources() + reader_resources{0, _total_memory - distributed_memory});
    });
}

// The call to change_weight is serialized as a consequence of the call to adjust.
future<> reader_concurrency_semaphore_group::change_weight(scheduling_group sg, size_t new_weight) {
   return change_weight(_semaphores.at(sg), new_weight);
}

// The call to change_weight is serialized as a consequence of the call to adjust.
future<> reader_concurrency_semaphore_group::change_weight(weighted_reader_concurrency_semaphore& sem, size_t new_weight) {
    auto diff = new_weight - sem.weight;
    if (diff) {
        auto old_weight = sem.weight;
        sem.weight += diff;
        _total_weight += diff;
        return adjust();
    }
    return make_ready_future<>();
}

future<> reader_concurrency_semaphore_group::set_memory(ssize_t new_memory_amount) {
    if (_total_memory == new_memory_amount) {
        return make_ready_future<>();
    }

    return with_semaphore(_operations_serializer, 1, [this, new_memory_amount] {
        ssize_t memory_delta = new_memory_amount - _total_memory;
        _total_memory = new_memory_amount;
        if (memory_delta > 0) {
            _spare_memory += memory_delta;
        }

        if (_spare_memory) {
            semaphore_priority_type memory_short_semaphores(priority_compare(*this));
            for (auto&& item : _semaphores) {
                auto& [sg, sem] = item;
                memory_short_semaphores.push(&sem);
            }
            distribute_spare_memory(memory_short_semaphores);
        }
        if (memory_delta > 0) {
            return make_ready_future<>();
        }
        return parallel_for_each(_semaphores, [this] (auto&& item) {
            auto& [sg, sem] = item;
            ssize_t delta = calc_delta(sem);
            if (delta < 0) {
                return reduce_memory(sem, std::abs(delta));
            }
            return make_ready_future<>();
        });
    });
}

future<> reader_concurrency_semaphore_group::wait_adjust_complete() {
    return with_semaphore(_operations_serializer, 1, [] {
        return make_ready_future<>();
    });
}

future<> reader_concurrency_semaphore_group::stop() noexcept {
    return parallel_for_each(_semaphores, [this] (auto&& item) {
        return item.second.sem.stop();
    }).then([this] {
        _semaphores.clear();
    });
}

reader_concurrency_semaphore& reader_concurrency_semaphore_group::get(scheduling_group sg) {
    return _semaphores.at(sg).sem;
}
reader_concurrency_semaphore* reader_concurrency_semaphore_group::get_or_null(scheduling_group sg) {
    auto it = _semaphores.find(sg);
    if (it == _semaphores.end()) {
        return nullptr;
    } else {
        return &(it->second.sem);
    }
}
reader_concurrency_semaphore& reader_concurrency_semaphore_group::add_or_update(scheduling_group sg, size_t shares) {
    auto result = _semaphores.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(sg),
        std::forward_as_tuple(
            0,
            _max_concurrent_reads,
            sg.name() + "_read_concurrency_sem",
            _max_queue_length
        ));
    auto&& it = result.first;
    // since we serialize all group changes this change wait will be queues and no further operations
    // will be executed until this adjustment ends.
    (void)change_weight(it->second, shares);
    return it->second.sem;
}

future<> reader_concurrency_semaphore_group::remove(scheduling_group sg) {
    auto node_handle =  _semaphores.extract(sg);
    if (!node_handle.empty()) {
        weighted_reader_concurrency_semaphore& sem = node_handle.mapped();
        return sem.sem.stop().then([this, &sem] {
            return change_weight(sem, 0);
        }).finally([node_handle = std::move(node_handle)] () {
            // this holds on to the node handle until we destroy it only after the semaphore
            // is stopped properly.
        });
    }
    return make_ready_future();
}

size_t reader_concurrency_semaphore_group::size() {
    return _semaphores.size();
}

void reader_concurrency_semaphore_group::foreach_semaphore(std::function<void(scheduling_group, reader_concurrency_semaphore&)> func) {
    for (auto& [sg, wsem] : _semaphores) {
        func(sg, wsem.sem);
    }
}

future<>
reader_concurrency_semaphore_group::foreach_semaphore_async(std::function<future<> (scheduling_group, reader_concurrency_semaphore&)> func) {
    auto units = co_await get_units(_operations_serializer, 1);
    for (auto& [sg, wsem] : _semaphores) {
        co_await func(sg, wsem.sem);
    }
}
