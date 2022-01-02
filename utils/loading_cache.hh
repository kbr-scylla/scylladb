/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <chrono>
#include <unordered_map>
#include <memory_resource>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/join.hpp>

#include <seastar/core/seastar.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>

#include "exceptions/exceptions.hh"
#include "utils/loading_shared_values.hh"
#include "utils/chunked_vector.hh"
#include "log.hh"

namespace bi = boost::intrusive;

namespace utils {

enum class loading_cache_reload_enabled { no, yes };

template <typename Tp>
struct simple_entry_size {
    size_t operator()(const Tp& val) {
        return 1;
    }
};

struct do_nothing_loading_cache_stats {
    // Accounts events when entries are evicted from the unprivileged cache section due to size restriction.
    // These events are interesting because they are an indication of a cache pollution event.
    static void inc_unprivileged_on_cache_size_eviction() noexcept {};
};

/// \brief Loading cache is a cache that loads the value into the cache using the given asynchronous callback.
///
/// Each cached value if reloading is enabled (\tparam ReloadEnabled == loading_cache_reload_enabled::yes) is reloaded after
/// the "refresh" time period since it was loaded for the last time.
///
/// The values are going to be evicted from the cache if they are not accessed during the "expiration" period or haven't
/// been reloaded even once during the same period.
///
/// If "expiration" is set to zero - the caching is going to be disabled and get_XXX(...) is going to call the "loader" callback
/// every time in order to get the requested value.
///
/// \note In order to avoid the eviction of cached entries due to "aging" of the contained value the user has to choose
/// the "expiration" to be at least ("refresh" + "max load latency"). This way the value is going to stay in the cache and is going to be
/// read in a non-blocking way as long as it's frequently accessed. Note however that since reloading is an asynchronous
/// procedure it may get delayed by other running task. Therefore choosing the "expiration" too close to the ("refresh" + "max load latency")
/// value one risks to have his/her cache values evicted when the system is heavily loaded.
///
/// The cache is also limited in size and if adding the next value is going
/// to exceed the cache size limit the least recently used value(s) is(are) going to be evicted until the size of the cache
/// becomes such that adding the new value is not going to break the size limit. If the new entry's size is greater than
/// the cache size then the get_XXX(...) method is going to return a future with the loading_cache::entry_is_too_big exception.
///
/// The cache is comprised of 2 dynamic sections.
/// Total size of both sections should not exceed the maximum cache size.
/// New cache entry is always added to the unprivileged section.
/// After a cache entry is read more than SectionHitThreshold times it moves to the second (privileged) cache section.
/// Both sections' entries obey expiration and reload rules as explained above.
/// When cache entries need to be evicted due to a size restriction unprivileged section least recently used entries are evicted first.
/// If cache size is still too big event after there are no more entries in the unprivileged section the least recently used entries
/// from the privileged section are going to be evicted till the cache size restriction is met.
///
/// The size of the cache is defined as a sum of sizes of all cached entries.
/// The size of each entry is defined by the value returned by the \tparam EntrySize predicate applied on it.
///
/// The get(key) or get_ptr(key) methods ensures that the "loader" callback is called only once for each cached entry regardless of how many
/// callers are calling for the get_XXX(key) for the same "key" at the same time. Only after the value is evicted from the cache
/// it's going to be "loaded" in the context of get_XXX(key). As long as the value is cached get_XXX(key) is going to return the
/// cached value immediately and reload it in the background every "refresh" time period as described above.
///
/// \tparam Key type of the cache key
/// \tparam Tp type of the cached value
/// \tparam SectionHitThreshold number of hits after which a cache item is going to be moved to the privileged cache section.
/// \tparam ReloadEnabled if loading_cache_reload_enabled::yes allow reloading the values otherwise don't reload
/// \tparam EntrySize predicate to calculate the entry size
/// \tparam Hash hash function
/// \tparam EqualPred equality predicate
/// \tparam LoadingSharedValuesStats statistics incrementing class (see utils::loading_shared_values)
/// \tparam Alloc elements allocator
template<typename Key,
         typename Tp,
         int SectionHitThreshold = 0,
         loading_cache_reload_enabled ReloadEnabled = loading_cache_reload_enabled::no,
         typename EntrySize = simple_entry_size<Tp>,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename LoadingSharedValuesStats = utils::do_nothing_loading_shared_values_stats,
         typename LoadingCacheStats = utils::do_nothing_loading_cache_stats,
         typename Alloc = std::pmr::polymorphic_allocator<>>
class loading_cache {

using loading_cache_clock_type = seastar::lowres_clock;
using safe_link_list_hook = bi::list_base_hook<bi::link_mode<bi::safe_link>>;

class timestamped_val {
public:
    using value_type = Tp;
    using loading_values_type = typename utils::loading_shared_values<Key, timestamped_val, Hash, EqualPred, LoadingSharedValuesStats, 256>;
    class lru_entry;
    class value_ptr;

private:
    value_type _value;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;
    lru_entry* _lru_entry_ptr = nullptr; /// MRU item is at the front, LRU - at the back
    size_t _size = 0;

public:
    timestamped_val(value_type val)
        : _value(std::move(val))
        , _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
        , _size(EntrySize()(_value))
    {}
    timestamped_val(timestamped_val&&) = default;

    timestamped_val& operator=(value_type new_val) {
        assert(_lru_entry_ptr);

        _value = std::move(new_val);
        _loaded = loading_cache_clock_type::now();
        _lru_entry_ptr->cache_size() -= _size;
        _size = EntrySize()(_value);
        _lru_entry_ptr->cache_size() += _size;
        return *this;
    }

    value_type& value() noexcept { return _value; }
    const value_type& value() const noexcept { return _value; }

    static const timestamped_val& container_of(const value_type& value) {
        return *bi::get_parent_from_member(&value, &timestamped_val::_value);
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }

    size_t size() const {
        return _size;
    }

    bool ready() const noexcept {
        return _lru_entry_ptr;
    }

    lru_entry* lru_entry_ptr() const noexcept {
        return _lru_entry_ptr;
    }

private:
    void touch() noexcept {
        assert(_lru_entry_ptr);
        _last_read = loading_cache_clock_type::now();
        _lru_entry_ptr->touch();
    }

    void set_anchor_back_reference(lru_entry* lru_entry_ptr) noexcept {
        _lru_entry_ptr = lru_entry_ptr;
    }
};

private:
    using loading_values_type = typename timestamped_val::loading_values_type;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;
    using ts_value_lru_entry = typename timestamped_val::lru_entry;
    using lru_list_type = typename ts_value_lru_entry::lru_list_type;
    using list_iterator = typename lru_list_type::iterator;

public:
    using value_type = Tp;
    using key_type = Key;
    using value_ptr = typename timestamped_val::value_ptr;

    class entry_is_too_big : public std::exception {};

private:
    loading_cache(size_t max_size, lowres_clock::duration expiry, lowres_clock::duration refresh, logging::logger& logger)
        : _max_size(max_size)
        , _expiry(expiry)
        , _refresh(refresh)
        , _logger(logger)
        , _timer([this] { on_timer(); })
    {
        static_assert(noexcept(LoadingCacheStats::inc_unprivileged_on_cache_size_eviction()), "LoadingCacheStats::inc_unprivileged_on_cache_size_eviction must be non-throwing");

        // Sanity check: if expiration period is given then non-zero refresh period and maximal size are required
        if (caching_enabled() && (_refresh == lowres_clock::duration(0) || _max_size == 0)) {
            throw exceptions::configuration_exception("loading_cache: caching is enabled but refresh period and/or max_size are zero");
        }
    }

public:
    template<typename Func>
    loading_cache(size_t max_size, lowres_clock::duration expiry, lowres_clock::duration refresh, logging::logger& logger, Func&& load)
        : loading_cache(max_size, expiry, refresh, logger)
    {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "This constructor should only be invoked when ReloadEnabled == loading_cache_reload_enabled::yes");
        static_assert(std::is_same<future<value_type>, std::result_of_t<Func(const key_type&)>>::value, "Bad Func signature");

        _load = std::forward<Func>(load);

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        _timer_period = std::min(_expiry, _refresh);
        _timer.arm(_timer_period);
    }

    loading_cache(size_t max_size, lowres_clock::duration expiry, logging::logger& logger)
        : loading_cache(max_size, expiry, loading_cache_clock_type::time_point::max().time_since_epoch(), logger)
    {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::no, "This constructor should only be invoked when ReloadEnabled == loading_cache_reload_enabled::no");

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        _timer_period = _expiry;
        _timer.arm(_timer_period);
    }

    ~loading_cache() {
        auto value_destroyer = [] (ts_value_lru_entry* ptr) { loading_cache::destroy_ts_value(ptr); };
        _unprivileged_lru_list.erase_and_dispose(_unprivileged_lru_list.begin(), _unprivileged_lru_list.end(), value_destroyer);
        _lru_list.erase_and_dispose(_lru_list.begin(), _lru_list.end(), value_destroyer);
    }

    template <typename LoadFunc>
    future<value_ptr> get_ptr(const Key& k, LoadFunc&& load) {
        static_assert(std::is_same<future<value_type>, std::result_of_t<LoadFunc(const key_type&)>>::value, "Bad LoadFunc signature");
        // We shouldn't be here if caching is disabled
        assert(caching_enabled());

        return _loading_values.get_or_load(k, [this, load = std::forward<LoadFunc>(load)] (const Key& k) mutable {
            return load(k).then([this] (value_type val) {
                return timestamped_val(std::move(val));
            });
        }).then([this, k] (timestamped_val_ptr ts_val_ptr) {
            // check again since it could have already been inserted and initialized
            if (!ts_val_ptr->ready()) {
                _logger.trace("{}: storing the value for the first time", k);

                if (ts_val_ptr->size() > _max_size) {
                    return make_exception_future<value_ptr>(entry_is_too_big());
                }

                ts_value_lru_entry* new_lru_entry = Alloc().template allocate_object<ts_value_lru_entry>();

                // Remove the least recently used items if map is too big.
                shrink();

                new(new_lru_entry) ts_value_lru_entry(std::move(ts_val_ptr), *this);

                // This will "touch" the entry and add it to the LRU list - we must do this before the shrink() call.
                value_ptr vp(new_lru_entry->timestamped_value_ptr());

                return make_ready_future<value_ptr>(std::move(vp));
            }

            return make_ready_future<value_ptr>(std::move(ts_val_ptr));
        });
    }

    future<value_ptr> get_ptr(const Key& k) {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "");
        return get_ptr(k, _load);
    }

    future<Tp> get(const Key& k) {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "");

        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        return get_ptr(k).then([] (value_ptr v_ptr) {
            return make_ready_future<Tp>(*v_ptr);
        });
    }

    future<> stop() {
        return _timer_reads_gate.close().finally([this] { _timer.cancel(); });
    }

    /// Find a value for a specific Key value and touch() it.
    /// \tparam KeyType Key type
    /// \tparam KeyHasher Hash functor type
    /// \tparam KeyEqual Equality functor type
    ///
    /// \param key Key value to look for
    /// \param key_hasher_func Hash functor
    /// \param key_equal_func Equality functor
    /// \return cache_value_ptr object pointing to the found value or nullptr otherwise.
    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    value_ptr find(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        // cache_value_ptr constructor is going to update a "last read" timestamp of the corresponding object and move
        // the object to the front of the LRU
        return set_find(key, std::move(key_hasher_func), std::move(key_equal_func));
    };

    value_ptr find(const Key& k) noexcept {
        return set_find(k);
    }

    template <typename Pred>
    void remove_if(Pred&& pred) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(const value_type&)>>::value, "Bad Pred signature");

        auto cond_pred = [this, &pred] (const ts_value_lru_entry& v) {
            return pred(v.timestamped_value().value());
        };
        auto value_destroyer = [this] (ts_value_lru_entry* p) {
            loading_cache::destroy_ts_value(p);
        };

        _unprivileged_lru_list.remove_and_dispose_if(cond_pred, value_destroyer);
        _lru_list.remove_and_dispose_if(cond_pred, value_destroyer);
    }

    void remove(const Key& k) {
        remove_ts_value(set_find(k));
    }

    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    void remove(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        remove_ts_value(set_find(key, std::move(key_hasher_func), std::move(key_equal_func)));
    }

    size_t size() const {
        return _lru_list.size() + _unprivileged_lru_list.size();
    }

    /// \brief returns the memory size the currently cached entries occupy according to the EntrySize predicate.
    size_t memory_footprint() const {
        return _current_size;
    }

private:
    void remove_ts_value(timestamped_val_ptr ts_ptr) {
        if (!ts_ptr) {
            return;
        }
        ts_value_lru_entry* lru_entry_ptr = ts_ptr->lru_entry_ptr();
        lru_list_type& entry_list = container_list(*lru_entry_ptr);
        entry_list.erase_and_dispose(entry_list.iterator_to(*lru_entry_ptr), [this] (ts_value_lru_entry* p) { loading_cache::destroy_ts_value(p); });
    }

    timestamped_val_ptr ready_entry_ptr(timestamped_val_ptr tv_ptr) {
        if (!tv_ptr || !tv_ptr->ready()) {
            return nullptr;
        }
        return std::move(tv_ptr);
    }

    lru_list_type& container_list(const ts_value_lru_entry& lru_entry_ptr) noexcept {
        return (lru_entry_ptr.touch_count() > SectionHitThreshold) ? _lru_list : _unprivileged_lru_list;
    }

    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    timestamped_val_ptr set_find(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        return ready_entry_ptr(_loading_values.find(key, std::move(key_hasher_func), std::move(key_equal_func)));
    }

    // keep the default non-templated overloads to ease on the compiler for specifications
    // that do not require the templated find().
    timestamped_val_ptr set_find(const Key& key) noexcept {
        return ready_entry_ptr(_loading_values.find(key));
    }

    bool caching_enabled() const {
        return _expiry != lowres_clock::duration(0);
    }

    static void destroy_ts_value(ts_value_lru_entry* val) {
        Alloc().delete_object(val);
    }

    /// This is the core method in the 2 sections LRU implementation.
    /// Set the given item as the most recently used item at the corresponding cache section.
    /// The MRU item is going to be at the front of the list, the LRU item - at the back.
    /// The entry is initially entering the "unprivileged" section (represented by a _unprivileged_lru_list).
    /// After an entry is touched more than SectionHitThreshold times it moves to a "privileged" section
    /// (represented by an _lru_list).
    ///
    /// \param lru_entry Cache item that has been "touched"
    void touch_lru_entry_2_sections(ts_value_lru_entry& lru_entry) {
        if (lru_entry.is_linked()) {
            lru_list_type& lru_list = container_list(lru_entry);
            lru_list.erase(lru_list.iterator_to(lru_entry));
        }

        if (lru_entry.touch_count() < SectionHitThreshold) {
            _logger.trace("Putting key {} into the unpriviledged section", lru_entry.key());
            _unprivileged_lru_list.push_front(lru_entry);
            lru_entry.inc_touch_count();
        } else {
            _logger.trace("Putting key {} into the priviledged section", lru_entry.key());
            _lru_list.push_front(lru_entry);

            // Bump it up only once to avoid a wrap around
            if (lru_entry.touch_count() == SectionHitThreshold) {
                lru_entry.inc_touch_count();
            }
        }
    }

    future<> reload(timestamped_val_ptr ts_value_ptr) {
        const Key& key = loading_values_type::to_key(ts_value_ptr);

        // Do nothing if the entry has been dropped before we got here (e.g. by the _load() call on another key that is
        // also being reloaded).
        if (!ts_value_ptr->lru_entry_ptr()) {
            _logger.trace("{}: entry was dropped before the reload", key);
            return make_ready_future<>();
        }

        return _load(key).then_wrapped([this, ts_value_ptr = std::move(ts_value_ptr), &key] (auto&& f) mutable {
            // if the entry has been evicted by now - simply end here
            if (!ts_value_ptr->lru_entry_ptr()) {
                _logger.trace("{}: entry was dropped during the reload", key);
                return make_ready_future<>();
            }

            // The exceptions are related to the load operation itself.
            // We should ignore them for the background reads - if
            // they persist the value will age and will be reloaded in
            // the forground. If the foreground READ fails the error
            // will be propagated up to the user and will fail the
            // corresponding query.
            try {
                *ts_value_ptr = f.get0();
            } catch (std::exception& e) {
                _logger.debug("{}: reload failed: {}", key, e.what());
            } catch (...) {
                _logger.debug("{}: reload failed: unknown error", key);
            }

            return make_ready_future<>();
        });
    }

    void drop_expired() {
        auto now = loading_cache_clock_type::now();
        auto expiration_cond = [now, this] (const ts_value_lru_entry& lru_entry) {
            using namespace std::chrono;
            // An entry should be discarded if it hasn't been reloaded for too long or nobody cares about it anymore
            const timestamped_val& v = lru_entry.timestamped_value();
            auto since_last_read = now - v.last_read();
            auto since_loaded = now - v.loaded();
            if (_expiry < since_last_read || (ReloadEnabled == loading_cache_reload_enabled::yes && _expiry < since_loaded)) {
                _logger.trace("drop_expired(): {}: dropping the entry: _expiry {},  ms passed since: loaded {} last_read {}", lru_entry.key(), _expiry.count(), duration_cast<milliseconds>(since_loaded).count(), duration_cast<milliseconds>(since_last_read).count());
                return true;
            }
            return false;
        };
        auto value_destroyer = [this] (ts_value_lru_entry* p) {
            loading_cache::destroy_ts_value(p);
        };

        _unprivileged_lru_list.remove_and_dispose_if(expiration_cond, value_destroyer);
        _lru_list.remove_and_dispose_if(expiration_cond, value_destroyer);
    }

    // Shrink the cache to the _max_size discarding the least recently used items.
    // Get rid from the entries that were used exactly once first.
    void shrink() {
        using namespace std::chrono;

        while (_current_size >= _max_size && !_unprivileged_lru_list.empty()) {
            ts_value_lru_entry& lru_entry = *_unprivileged_lru_list.rbegin();
            _logger.trace("shrink(): {}: dropping the unpriviledged entry: ms since last_read {}", lru_entry.key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - lru_entry.timestamped_value().last_read()).count());
            loading_cache::destroy_ts_value(&lru_entry);
            LoadingCacheStats::inc_unprivileged_on_cache_size_eviction();
        }

        while (_current_size >= _max_size) {
            ts_value_lru_entry& lru_entry = *_lru_list.rbegin();
            _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", lru_entry.key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - lru_entry.timestamped_value().last_read()).count());
            loading_cache::destroy_ts_value(&lru_entry);
        }
    }

    // Try to bring the load factors of the _loading_values into a known range.
    void periodic_rehash() noexcept {
        try {
            _loading_values.rehash();
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // check if rehashing is needed and do it if it is.
        periodic_rehash();

        if constexpr (ReloadEnabled == loading_cache_reload_enabled::no) {
            _logger.trace("on_timer(): rearming");
            _timer.arm(loading_cache_clock_type::now() + _timer_period);
            return;
        }

        // Reload all those which value needs to be reloaded.
        // Future is waited on indirectly in `stop()` (via `_timer_reads_gate`).
        // FIXME: error handling
        (void)with_gate(_timer_reads_gate, [this] {
            auto to_reload = boost::copy_range<utils::chunked_vector<timestamped_val_ptr>>(boost::range::join(_unprivileged_lru_list, _lru_list)
                    | boost::adaptors::filtered([this] (ts_value_lru_entry& lru_entry) {
                        return lru_entry.timestamped_value().loaded() + _refresh < loading_cache_clock_type::now();
                    })
                    | boost::adaptors::transformed([] (ts_value_lru_entry& lru_entry) {
                        return lru_entry.timestamped_value_ptr();
                    }));

            return parallel_for_each(std::move(to_reload), [this] (timestamped_val_ptr ts_value_ptr) {
                _logger.trace("on_timer(): {}: reloading the value", loading_values_type::to_key(ts_value_ptr));
                return this->reload(std::move(ts_value_ptr));
            }).finally([this] {
                _logger.trace("on_timer(): rearming");
                _timer.arm(loading_cache_clock_type::now() + _timer_period);
            });
        });
    }

    loading_values_type _loading_values;
    lru_list_type _lru_list;              // list containing "privileged" section entries
    lru_list_type _unprivileged_lru_list; // list containing "unprivileged" section entries
    size_t _current_size = 0;
    size_t _max_size = 0;
    loading_cache_clock_type::duration _expiry;
    loading_cache_clock_type::duration _refresh;
    loading_cache_clock_type::duration _timer_period;
    logging::logger& _logger;
    std::function<future<Tp>(const Key&)> _load;
    timer<loading_cache_clock_type> _timer;
    seastar::gate _timer_reads_gate;
};

template<typename Key, typename Tp, int SectionHitThreshold, loading_cache_reload_enabled ReloadEnabled, typename EntrySize, typename Hash, typename EqualPred, typename LoadingSharedValuesStats, typename LoadingCacheStats, typename Alloc>
class loading_cache<Key, Tp, SectionHitThreshold, ReloadEnabled, EntrySize, Hash, EqualPred, LoadingSharedValuesStats, LoadingCacheStats, Alloc>::timestamped_val::value_ptr {
private:
    using loading_values_type = typename timestamped_val::loading_values_type;

public:
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;
    using value_type = Tp;

private:
    timestamped_val_ptr _ts_val_ptr;

public:
    value_ptr(timestamped_val_ptr ts_val_ptr) : _ts_val_ptr(std::move(ts_val_ptr)) {
        if (_ts_val_ptr) {
            _ts_val_ptr->touch();
        }
    }
    value_ptr(std::nullptr_t) noexcept : _ts_val_ptr() {}
    bool operator==(const value_ptr& x) const { return _ts_val_ptr == x._ts_val_ptr; }
    bool operator!=(const value_ptr& x) const { return !operator==(x); }
    explicit operator bool() const noexcept { return bool(_ts_val_ptr); }
    value_type& operator*() const noexcept { return _ts_val_ptr->value(); }
    value_type* operator->() const noexcept { return &_ts_val_ptr->value(); }

    friend std::ostream& operator<<(std::ostream& os, const value_ptr& vp) {
        return os << vp._ts_val_ptr;
    }
};

/// \brief This is and LRU list entry which is also an anchor for a loading_cache value.
template<typename Key, typename Tp, int SectionHitThreshold, loading_cache_reload_enabled ReloadEnabled, typename EntrySize, typename Hash, typename EqualPred, typename LoadingSharedValuesStats, typename  LoadingCacheStats, typename Alloc>
class loading_cache<Key, Tp, SectionHitThreshold, ReloadEnabled, EntrySize, Hash, EqualPred, LoadingSharedValuesStats, LoadingCacheStats, Alloc>::timestamped_val::lru_entry : public safe_link_list_hook {
private:
    using loading_values_type = typename timestamped_val::loading_values_type;

public:
    using lru_list_type = bi::list<lru_entry>;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;

private:
    timestamped_val_ptr _ts_val_ptr;
    loading_cache& _parent;
    int _touch_count;

public:
    lru_entry(timestamped_val_ptr ts_val, loading_cache& owner_cache)
        : _ts_val_ptr(std::move(ts_val))
        , _parent(owner_cache)
        , _touch_count(0)
    {
        // We don't want to allow SectionHitThreshold to be greater than half the max value of _touch_count to avoid a wrap around
        static_assert(SectionHitThreshold <= std::numeric_limits<typeof(_touch_count)>::max() / 2, "SectionHitThreshold value is too big");

        _ts_val_ptr->set_anchor_back_reference(this);
        cache_size() += _ts_val_ptr->size();
    }

    void inc_touch_count() noexcept {
        ++_touch_count;
    }

    int touch_count() const noexcept {
        return _touch_count;
    }

    ~lru_entry() {
        if (safe_link_list_hook::is_linked()) {
            lru_list_type& lru_list = _parent.container_list(*this);
            lru_list.erase(lru_list.iterator_to(*this));
        }
        cache_size() -= _ts_val_ptr->size();
        _ts_val_ptr->set_anchor_back_reference(nullptr);
    }

    size_t& cache_size() noexcept {
        return _parent._current_size;
    }

    void touch() noexcept {
        _parent.touch_lru_entry_2_sections(*this);
    }

    const Key& key() const noexcept {
        return loading_values_type::to_key(_ts_val_ptr);
    }

    timestamped_val& timestamped_value() noexcept { return *_ts_val_ptr; }
    const timestamped_val& timestamped_value() const noexcept { return *_ts_val_ptr; }
    timestamped_val_ptr timestamped_value_ptr() noexcept { return _ts_val_ptr; }
};

}

