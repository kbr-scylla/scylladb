/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "large_bitset.hh"
#include <algorithm>
#include <seastar/core/align.hh>
#include "seastarx.hh"

large_bitset::large_bitset(size_t nr_bits) : _nr_bits(nr_bits) {
    auto nr_blocks = align_up(nr_bits, bits_per_block()) / bits_per_block();
    _storage.reserve(nr_blocks);
    size_t nr_ints = align_up(nr_bits, bits_per_int()) / bits_per_int();
    while (nr_ints) {
        auto now = std::min(ints_per_block(), nr_ints);
        _storage.push_back(std::make_unique<int_type[]>(now));
        std::fill_n(_storage.back().get(), now, 0);
        nr_ints -= now;
    }
}

void
large_bitset::clear() {
    size_t nr_ints = align_up(_nr_bits, bits_per_int()) / bits_per_int();
    auto bp = _storage.begin();
    while (nr_ints) {
        auto now = std::min(ints_per_block(), nr_ints);
        std::fill_n(bp++->get(), now, 0);
        nr_ints -= now;
    }
}
