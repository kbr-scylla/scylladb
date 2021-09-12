/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <iosfwd>

#include "mutation_partition.hh"
#include "keys.hh"
#include "schema_fwd.hh"
#include "dht/i_partitioner.hh"
#include "hashing.hh"
#include "mutation_fragment.hh"
#include "mutation_consumer_concepts.hh"

#include <seastar/util/optimized_optional.hh>


template<typename Result>
struct mutation_consume_result {
    stop_iteration stop;
    Result result;
};

template<>
struct mutation_consume_result<void> {
    stop_iteration stop;
};

enum class consume_in_reverse {
    no = 0,
    yes,
    legacy_half_reverse,
};

class mutation final {
private:
    struct data {
        schema_ptr _schema;
        dht::decorated_key _dk;
        mutation_partition _p;

        data(dht::decorated_key&& key, schema_ptr&& schema);
        data(partition_key&& key, schema_ptr&& schema);
        data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp);
        data(schema_ptr&& schema, dht::decorated_key&& key, mutation_partition&& mp);
    };
    std::unique_ptr<data> _ptr;
private:
    mutation() = default;
    explicit operator bool() const { return bool(_ptr); }
    friend class optimized_optional<mutation>;
public:
    mutation(schema_ptr schema, dht::decorated_key key)
        : _ptr(std::make_unique<data>(std::move(key), std::move(schema)))
    { }
    mutation(schema_ptr schema, partition_key key_)
        : _ptr(std::make_unique<data>(std::move(key_), std::move(schema)))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, const mutation_partition& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), mp))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, mutation_partition&& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), std::move(mp)))
    { }
    mutation(const mutation& m)
    {
        if (m._ptr) {
            _ptr = std::make_unique<data>(schema_ptr(m.schema()), dht::decorated_key(m.decorated_key()), m.partition());
        }
    }
    mutation(mutation&&) = default;
    mutation& operator=(mutation&& x) = default;
    mutation& operator=(const mutation& m);

    void set_static_cell(const column_definition& def, atomic_cell_or_collection&& value);
    void set_static_cell(const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value);
    void set_cell(const clustering_key_prefix& prefix, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value);

    // Upgrades this mutation to a newer schema. The new schema must
    // be obtained using only valid schema transformation:
    //  * primary key column count must not change
    //  * column types may only change to those with compatible representations
    //
    // After upgrade, mutation's partition should only be accessed using the new schema. User must
    // ensure proper isolation of accesses.
    //
    // Strong exception guarantees.
    //
    // Note that the conversion may lose information, it's possible that m1 != m2 after:
    //
    //   auto m2 = m1;
    //   m2.upgrade(s2);
    //   m2.upgrade(m1.schema());
    //
    void upgrade(const schema_ptr&);

    const partition_key& key() const { return _ptr->_dk._key; };
    const dht::decorated_key& decorated_key() const { return _ptr->_dk; };
    dht::ring_position ring_position() const { return { decorated_key() }; }
    const dht::token& token() const { return _ptr->_dk._token; }
    const schema_ptr& schema() const { return _ptr->_schema; }
    const mutation_partition& partition() const { return _ptr->_p; }
    mutation_partition& partition() { return _ptr->_p; }
    const utils::UUID& column_family_id() const { return _ptr->_schema->id(); }
    // Consistent with hash<canonical_mutation>
    bool operator==(const mutation&) const;
    bool operator!=(const mutation&) const;
public:
    // Consumes the mutation's content.
    //
    // The mutation is in a moved-from alike state after consumption.
    // There are tree ways to consume the mutation:
    // * consume_in_reverse::no - consume in forward order, as defined by the
    //   schema.
    // * consume_in_reverse::yes - consume in reverse order, as if the schema
    //   had the opposite clustering order. This effectively reverses the
    //   mutation's content, according to the native reverse order[1].
    // * consume_in_reverse::legacy_half_reverse - consume rows and range
    //   tombstones in legacy reverse order[2].
    //
    // For definition of [1] and [2] see docs/design-notes/reverse-reads.md.
    template<FlattenedConsumer Consumer>
    auto consume(Consumer& consumer, consume_in_reverse reverse) && -> mutation_consume_result<decltype(consumer.consume_end_of_stream())>;

    // See mutation_partition::live_row_count()
    uint64_t live_row_count(gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    void apply(mutation&&);
    void apply(const mutation&);
    void apply(const mutation_fragment&);

    mutation operator+(const mutation& other) const;
    mutation& operator+=(const mutation& other);
    mutation& operator+=(mutation&& other);

    // Returns a subset of this mutation holding only information relevant for given clustering ranges.
    // Range tombstones will be trimmed to the boundaries of the clustering ranges.
    mutation sliced(const query::clustering_row_ranges&) const;
private:
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};

namespace {

template<consume_in_reverse reverse, FlattenedConsumer Consumer>
stop_iteration consume_clustering_fragments(schema_ptr s, mutation_partition& partition, Consumer& consumer) {
    using crs_type = mutation_partition::rows_type;
    using crs_iterator_type = std::conditional_t<reverse == consume_in_reverse::legacy_half_reverse || reverse == consume_in_reverse::yes, crs_type::reverse_iterator, crs_type::iterator>;
    using rts_type = range_tombstone_list;
    using rts_iterator_type = std::conditional_t<reverse == consume_in_reverse::legacy_half_reverse, rts_type::reverse_iterator, rts_type::iterator>;

    if constexpr (reverse == consume_in_reverse::yes) {
        s = s->make_reversed();
    }

    // only used when reverse == consume_in_reverse::yes
    range_tombstone_list reversed_range_tombstones(*s);

    crs_iterator_type crs_it, crs_end;
    rts_iterator_type rts_it, rts_end;
    if constexpr (reverse == consume_in_reverse::legacy_half_reverse) {
        crs_it = partition.clustered_rows().rbegin();
        crs_end = partition.clustered_rows().rend();
        rts_it = partition.row_tombstones().rbegin();
        rts_end = partition.row_tombstones().rend();
    } else if constexpr (reverse == consume_in_reverse::yes) {
        crs_it = partition.clustered_rows().rbegin();
        crs_end = partition.clustered_rows().rend();

        while (!partition.row_tombstones().empty()) {
            auto rt = partition.mutable_row_tombstones().pop_front_and_lock();
            rt.reverse();
            reversed_range_tombstones.apply(*s, std::move(rt));
        }
        rts_it = reversed_range_tombstones.begin();
        rts_end = reversed_range_tombstones.end();
    } else {
        crs_it = partition.clustered_rows().begin();
        crs_end = partition.clustered_rows().end();
        rts_it = partition.row_tombstones().begin();
        rts_end = partition.row_tombstones().end();
    }

    stop_iteration stop = stop_iteration::no;

    position_in_partition::tri_compare cmp(*s);

    while (!stop && (crs_it != crs_end || rts_it != rts_end)) {
        bool emit_rt;
        if (crs_it != crs_end && rts_it != rts_end) {
            const auto cmp_res = cmp(rts_it->position(), crs_it->position());
            if constexpr (reverse == consume_in_reverse::legacy_half_reverse) {
                emit_rt = cmp_res > 0;
            } else {
                emit_rt = cmp_res < 0;
            }
        } else {
            emit_rt = rts_it != rts_end;
        }
        if (emit_rt) {
            stop = consumer.consume(std::move(rts_it->tombstone()));
            ++rts_it;
        } else {
            // Dummy rows are part of the in-memory representation but should be
            // invisible to reads.
            if (!crs_it->dummy()) {
                stop = consumer.consume(clustering_row(std::move(*crs_it)));
            }
            ++crs_it;
        }
    }

    return stop;
}

} // anonymous namespace

template<FlattenedConsumer Consumer>
auto mutation::consume(Consumer& consumer, consume_in_reverse reverse) && -> mutation_consume_result<decltype(consumer.consume_end_of_stream())> {
    consumer.consume_new_partition(_ptr->_dk);

    auto& partition = _ptr->_p;

    if (partition.partition_tombstone()) {
        consumer.consume(partition.partition_tombstone());
    }

    stop_iteration stop = stop_iteration::no;
    if (!partition.static_row().empty()) {
        stop = consumer.consume(static_row(std::move(partition.static_row().get_existing())));
    }

    if (reverse == consume_in_reverse::yes) {
        stop = consume_clustering_fragments<consume_in_reverse::yes>(_ptr->_schema, partition, consumer);
    } else if (reverse == consume_in_reverse::legacy_half_reverse) {
        stop = consume_clustering_fragments<consume_in_reverse::legacy_half_reverse>(_ptr->_schema, partition, consumer);
    } else {
        stop = consume_clustering_fragments<consume_in_reverse::no>(_ptr->_schema, partition, consumer);
    }

    const auto stop_consuming = consumer.consume_end_of_partition();
    using consume_res_type = decltype(consumer.consume_end_of_stream());
    if constexpr (std::is_same_v<consume_res_type, void>) {
        consumer.consume_end_of_stream();
        return mutation_consume_result<void>{stop_consuming};
    } else {
        return mutation_consume_result<consume_res_type>{stop_consuming, consumer.consume_end_of_stream()};
    }
}

struct mutation_equals_by_key {
    bool operator()(const mutation& m1, const mutation& m2) const {
        return m1.schema() == m2.schema()
                && m1.decorated_key().equal(*m1.schema(), m2.decorated_key());
    }
};

struct mutation_hash_by_key {
    size_t operator()(const mutation& m) const {
        auto dk_hash = std::hash<dht::decorated_key>();
        return dk_hash(m.decorated_key());
    }
};

struct mutation_decorated_key_less_comparator {
    bool operator()(const mutation& m1, const mutation& m2) const;
};

using mutation_opt = optimized_optional<mutation>;

// Consistent with operator==()
// Consistent across the cluster, so should not rely on particular
// serialization format, only on actual data stored.
template<>
struct appending_hash<mutation> {
    template<typename Hasher>
    void operator()(Hasher& h, const mutation& m) const {
        const schema& s = *m.schema();
        feed_hash(h, m.key(), s);
        m.partition().feed_hash(h, s);
    }
};

inline
void apply(mutation_opt& dst, mutation&& src) {
    if (!dst) {
        dst = std::move(src);
    } else {
        dst->apply(std::move(src));
    }
}

inline
void apply(mutation_opt& dst, mutation_opt&& src) {
    if (src) {
        apply(dst, std::move(*src));
    }
}

// Returns a range into partitions containing mutations covered by the range.
// partitions must be sorted according to decorated key.
// range must not wrap around.
boost::iterator_range<std::vector<mutation>::const_iterator> slice(
    const std::vector<mutation>& partitions,
    const dht::partition_range&);

class flat_mutation_reader;

// Reads a single partition from a reader. Returns empty optional if there are no more partitions to be read.
future<mutation_opt> read_mutation_from_flat_mutation_reader(flat_mutation_reader& reader);

// Reverses the mutation as if it was created with a schema with reverse
// clustering order. The resulting mutation will contain a reverse schema too.
mutation reverse(mutation mut);
