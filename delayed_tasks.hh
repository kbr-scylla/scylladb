/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <list>
#include <stdexcept>

#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

#include "log.hh"
#include "seastarx.hh"

//
// Delay asynchronous tasks.
//
template <class Clock = std::chrono::steady_clock>
class delayed_tasks final {
    static logging::logger _logger;

    // A waiter is destroyed before the timer has elapsed.
    class cancelled : public std::exception {
    public:
    };

    class waiter final {
        timer<Clock> _timer;

        promise<> _done{};

    public:
        explicit waiter(typename Clock::duration d) : _timer([this] { _done.set_value(); }) {
            _timer.arm(d);
        }

        ~waiter() {
            if (_timer.armed()) {
                _timer.cancel();
                _done.set_exception(cancelled());
            }
        }

        future<> get_future() noexcept {
            return _done.get_future();
        }
    };

    // `std::list` because iterators are not invalidated. We assume that look-up time is not a bottleneck.
    std::list<std::unique_ptr<waiter>> _waiters{};

public:
    //
    // Schedule the task `f` after d` has elapsed. If the instance goes out of scope before
    // the duration has elapsed, then the task is cancelled.
    //
    template <class Rep, class Period, class Task>
    void schedule_after(std::chrono::duration<Rep, Period> d, Task&& f) {
        _logger.trace("Adding scheduled task.");

        auto iter = _waiters.insert(_waiters.end(), std::make_unique<waiter>(d));
        auto& w = *iter;

        w->get_future().then([this, f = std::move(f)] {
            _logger.trace("Running scheduled task.");
            return f();
        }).then([this, iter] {
             // We'll only get here if the instance is still alive, since otherwise the future will be resolved to
             // `cancelled`.
            _waiters.erase(iter);
        }).template handle_exception_type([](const cancelled&) {
            // Nothing.
            return make_ready_future<>();
        });
    }

    //
    // Cancel all scheduled tasks.
    //
    void cancel_all() {
        _waiters.clear();
    }
};

template <class Clock>
logging::logger delayed_tasks<Clock>::_logger("delayed_tasks");
