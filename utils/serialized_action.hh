/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <functional>
#include <seastar/core/semaphore.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/later.hh>

// An async action wrapper which ensures that at most one action
// is running at any time.
class serialized_action {
public:
    template <typename... T>
    using future = seastar::future<T...>;
private:
    std::function<future<>()> _func;
    seastar::shared_future<> _pending;
    seastar::semaphore _sem;
private:
    future<> do_trigger() {
        _pending = {};
        return futurize_invoke(_func);
    }
public:
    serialized_action(std::function<future<>()> func)
        : _func(std::move(func))
        , _sem(1)
    { }
    serialized_action(serialized_action&&) = delete;
    serialized_action(const serialized_action&) = delete;

    // Makes sure that a new action will be started after this call and
    // returns a future which resolves when that action completes.
    // At most one action can run at any given moment.
    // A single action is started on behalf of all earlier triggers.
    //
    // When action is not currently running, it is started immediately if !later or
    // at some point in time soon after current fiber defers when later is true.
    future<> trigger(bool later = false) {
        if (_pending.valid()) {
            return _pending;
        }
        seastar::shared_promise<> pr;
        _pending = pr.get_shared_future();
        future<> ret = _pending;
        // run in background, synchronize using `ret`
        (void)_sem.wait().then([this, later] () mutable {
            if (later) {
                return seastar::yield().then([this] () mutable {
                    return do_trigger();
                });
            }
            return do_trigger();
        }).then_wrapped([pr = std::move(pr)] (auto&& f) mutable {
            if (f.failed()) {
                pr.set_exception(f.get_exception());
            } else {
                pr.set_value();
            }
        });
        return ret.finally([this] {
            _sem.signal();
        });
    }

    // Like trigger(), but defers invocation of the action to allow for batching
    // more requests.
    future<> trigger_later() {
        return trigger(true);
    }

    // Waits for all invocations initiated in the past.
    future<> join() {
        return get_units(_sem, 1).discard_result();
    }
};
