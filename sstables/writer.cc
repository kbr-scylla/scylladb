/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "sstables.hh"
#include "sstable_writer.hh"
#include "writer.hh"
#include "mx/writer.hh"

namespace sstables {

sstable_writer::sstable_writer(sstable& sst, const schema& s, uint64_t estimated_partitions,
        const sstable_writer_config& cfg, encoding_stats enc_stats, const io_priority_class& pc, shard_id shard) {
    if (sst.get_version() < oldest_writable_sstable_format) {
        on_internal_error(sstlog, format("writing sstables with too old format: {}", sst.get_version()));
    }
    _impl = mc::make_writer(sst, s, estimated_partitions, cfg, enc_stats, pc, shard);
    if (cfg.replay_position) {
        _impl->_collector.set_replay_position(cfg.replay_position.value());
    }
    if (cfg.sstable_level) {
        _impl->_collector.set_sstable_level(cfg.sstable_level.value());
    }
}

void sstable_writer::consume_new_partition(const dht::decorated_key& dk) {
    _impl->_validator(dk);
    _impl->_validator(mutation_fragment::kind::partition_start, position_in_partition_view(position_in_partition_view::partition_start_tag_t{}));
    _impl->_sst.get_stats().on_partition_write();
    return _impl->consume_new_partition(dk);
}

void sstable_writer::consume(tombstone t) {
    _impl->_sst.get_stats().on_tombstone_write();
    return _impl->consume(t);
}

stop_iteration sstable_writer::consume(static_row&& sr) {
    _impl->_validator(mutation_fragment::kind::static_row, sr.position());
    if (!sr.empty()) {
        _impl->_sst.get_stats().on_static_row_write();
    }
    return _impl->consume(std::move(sr));
}

stop_iteration sstable_writer::consume(clustering_row&& cr) {
    _impl->_validator(mutation_fragment::kind::clustering_row, cr.position());
    _impl->_sst.get_stats().on_row_write();
    return _impl->consume(std::move(cr));
}

stop_iteration sstable_writer::consume(range_tombstone&& rt) {
    _impl->_validator(mutation_fragment::kind::range_tombstone, rt.position());
    _impl->_sst.get_stats().on_range_tombstone_write();
    return _impl->consume(std::move(rt));
}

stop_iteration sstable_writer::consume_end_of_partition() {
    _impl->_validator.on_end_of_partition();
    return _impl->consume_end_of_partition();
}

void sstable_writer::consume_end_of_stream() {
    _impl->_validator.on_end_of_stream();
    if (_impl->_c_stats.capped_local_deletion_time) {
        _impl->_sst.get_stats().on_capped_local_deletion_time();
    }
    return _impl->consume_end_of_stream();
}

sstable_writer::sstable_writer(sstable_writer&& o) = default;
sstable_writer& sstable_writer::operator=(sstable_writer&& o) = default;
sstable_writer::~sstable_writer() = default;

} // namespace sstables
