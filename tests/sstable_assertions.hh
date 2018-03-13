/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "schema.hh"
#include "sstables/index_reader.hh"

class index_reader_assertions {
    std::unique_ptr<sstables::index_reader> _r;
public:
    index_reader_assertions(std::unique_ptr<sstables::index_reader> r)
        : _r(std::move(r))
    { }

    index_reader_assertions& has_monotonic_positions(const schema& s) {
        auto pos_cmp = position_in_partition::composite_less_compare(s);
        auto rp_cmp = dht::ring_position_comparator(s);
        auto prev = dht::ring_position::min();
        _r->read_partition_data().get();
        while (!_r->eof()) {
            auto k = _r->current_partition_entry().get_decorated_key();
            auto rp = dht::ring_position(k.token(), k.key().to_partition_key(s));

            if (!rp_cmp(prev, rp)) {
                BOOST_FAIL(sprint("Partitions have invalid order: %s >= %s", prev, rp));
            }

            prev = rp;

            auto* pi = _r->current_partition_entry().get_promoted_index(s);
            if (!pi->entries.empty()) {
                auto& prev = pi->entries[0];
                for (size_t i = 1; i < pi->entries.size(); ++i) {
                    auto& cur = pi->entries[i];
                    if (pos_cmp(cur.start, prev.end)) {
                        std::cout << "promoted index:\n";
                        for (auto& e : pi->entries) {
                            std::cout << "  " << e.start << "-" << e.end << ": +" << e.offset << " len=" << e.width << std::endl;
                        }
                        BOOST_FAIL(sprint("Index blocks are not monotonic: %s >= %s", prev.end, cur.start));
                    }
                    cur = prev;
                }
            }
            _r->advance_to_next_partition().get();
        }
        return *this;
    }

    index_reader_assertions& is_empty(const schema& s) {
        _r->read_partition_data().get();
        while (!_r->eof()) {
            auto* pi = _r->current_partition_entry().get_promoted_index(s);
            BOOST_REQUIRE(pi == nullptr);
            _r->advance_to_next_partition().get();
        }
        return *this;
    }
};

inline
index_reader_assertions assert_that(std::unique_ptr<sstables::index_reader> r) {
    return { std::move(r) };
}
