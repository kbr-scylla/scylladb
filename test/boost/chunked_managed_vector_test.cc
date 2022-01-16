/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>

#include <deque>
#include <random>
#include "utils/lsa/chunked_managed_vector.hh"

#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/equal.hpp>
#include <boost/range/algorithm/reverse.hpp>
#include <boost/range/irange.hpp>

using namespace logalloc;

using disk_array = lsa::chunked_managed_vector<uint64_t>;

using deque = std::deque<int>;

SEASTAR_TEST_CASE(test_random_walk) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
    auto rand = std::default_random_engine();
    auto op_gen = std::uniform_int_distribution<unsigned>(0, 9);
    auto nr_dist = std::geometric_distribution<size_t>(0.7);
    deque d;
    disk_array c;
    for (auto i = 0; i != 1000000; ++i) {
        if (i % 10000 == 0) {
            region.full_compaction();
        }
        as(region, [&] {
        auto op = op_gen(rand);
        switch (op) {
            case 0: {
                auto n = rand();
                c.push_back(n);
                d.push_back(n);
                break;
            }
            case 1: {
                auto nr_pushes = nr_dist(rand);
                for (auto i : boost::irange(size_t(0), nr_pushes)) {
                    (void)i;
                    auto n = rand();
                    c.push_back(n);
                    d.push_back(n);
                }
                break;
            }
            case 2: {
                if (!d.empty()) {
                    auto n = d.back();
                    auto m = c.back();
                    BOOST_REQUIRE_EQUAL(n, m);
                    c.pop_back();
                    d.pop_back();
                }
                break;
            }
            case 3: {
                c.reserve(nr_dist(rand));
                break;
            }
            case 4: {
                boost::sort(c);
                boost::sort(d);
                break;
            }
            case 5: {
                if (!d.empty()) {
                    auto u = std::uniform_int_distribution<size_t>(0, d.size() - 1);
                    auto idx = u(rand);
                    auto m = c[idx];
                    auto n = c[idx];
                    BOOST_REQUIRE_EQUAL(m, n);
                }
                break;
            }
            case 6: {
                c.clear();
                d.clear();
                break;
            }
            case 7: {
                boost::reverse(c);
                boost::reverse(d);
                break;
            }
            case 8: {
                c.clear();
                d.clear();
                break;
            }
            case 9: {
                auto nr = nr_dist(rand);
                c.resize(nr);
                d.resize(nr);
                break;
            }
            default:
                abort();
        }
        });
        BOOST_REQUIRE_EQUAL(c.size(), d.size());
        BOOST_REQUIRE(boost::equal(c, d));
    }
  });
  return make_ready_future<>();
}

class exception_safety_checker {
    uint64_t _live_objects = 0;
    uint64_t _countdown = std::numeric_limits<uint64_t>::max();
public:
    bool ok() const {
        return !_live_objects;
    }
    void set_countdown(unsigned x) {
        _countdown = x;
    }
    void add_live_object() {
        if (!_countdown--) { // auto-clears
            throw "ouch";
        }
        ++_live_objects;
    }
    void del_live_object() {
        --_live_objects;
    }
};

class exception_safe_class {
    exception_safety_checker* _esc;
public:
    explicit exception_safe_class(exception_safety_checker& esc) : _esc(&esc) {
        _esc->add_live_object();
    }
    exception_safe_class(const exception_safe_class& x) : _esc(x._esc) {
        _esc->add_live_object();
    }
    exception_safe_class(exception_safe_class&&) = default;
    ~exception_safe_class() {
        _esc->del_live_object();
    }
    exception_safe_class& operator=(const exception_safe_class& x) {
        if (this != &x) {
            auto tmp = x;
            this->~exception_safe_class();
            *this = std::move(tmp);
        }
        return *this;
    }
    exception_safe_class& operator=(exception_safe_class&&) = default;
};

SEASTAR_TEST_CASE(tests_constructor_exception_safety) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
   as(region, [&] {
    auto checker = exception_safety_checker();
    auto v = std::vector<exception_safe_class>(100, exception_safe_class(checker));
    checker.set_countdown(5);
    try {
        auto u = lsa::chunked_managed_vector<exception_safe_class>(v.begin(), v.end());
        BOOST_REQUIRE(false);
    } catch (...) {
        v.clear();
        BOOST_REQUIRE(checker.ok());
    }
   });
  });
  return make_ready_future<>();
}

SEASTAR_TEST_CASE(tests_reserve_partial) {
  region region;
  allocating_section as;
  with_allocator(region.allocator(), [&] {
   as(region, [&] {
    auto rand = std::default_random_engine();
    auto size_dist = std::uniform_int_distribution<unsigned>(1, 1 << 12);

    for (int i = 0; i < 100; ++i) {
        lsa::chunked_managed_vector<uint8_t> v;
        const auto orig_size = size_dist(rand);
        auto size = orig_size;
        while (size) {
            size = v.reserve_partial(size);
        }
        BOOST_REQUIRE_EQUAL(v.capacity(), orig_size);
    }
   });
  });
  return make_ready_future<>();
}
