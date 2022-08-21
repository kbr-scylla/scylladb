/*
 * Copyright (C) 2019 ScyllaDB
 *
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once
#include "compaction_backlog_manager.hh"
#include "incremental_compaction_strategy.hh"
#include "sstables/sstable_set.hh"
#include <cmath>
#include <ctgmath>
#include <boost/range/adaptor/map.hpp>

using namespace sstables;

// The only difference to size tiered backlog tracker is that it will calculate
// backlog contribution using total bytes of each sstable run instead of total
// bytes of an individual sstable object.
class incremental_backlog_tracker final : public compaction_backlog_tracker::impl {
    incremental_compaction_strategy_options _options;
    int64_t _total_bytes = 0;
    int64_t _total_backlog_bytes = 0;
    unsigned _threshold = 0;
    double _sstables_backlog_contribution = 0.0f;
    std::unordered_set<sstables::run_id> _sstable_runs_contributing_backlog;
    std::unordered_map<sstables::run_id, sstable_run> _all;

    struct inflight_component {
        int64_t total_bytes = 0;
        double contribution = 0;
    };

    inflight_component compacted_backlog(const compaction_backlog_tracker::ongoing_compactions& ongoing_compactions) const {
        inflight_component in;
        for (auto& crp : ongoing_compactions) {
            if (!_sstable_runs_contributing_backlog.contains(crp.first->run_identifier())) {
                continue;
            }
            auto compacted = crp.second->compacted();
            in.total_bytes += compacted;
            in.contribution += compacted * log4((crp.first->data_size()));
        }
        return in;
    }
    double log4(double x) const {
        static const double inv_log_4 = 1.0f / std::log(4);
        return log(x) * inv_log_4;
    }

    void refresh_sstables_backlog_contribution() {
        _total_backlog_bytes = 0;
        _sstables_backlog_contribution = 0.0f;
        _sstable_runs_contributing_backlog = {};
        if (_all.empty()) {
            return;
        }

        for (auto& bucket : incremental_compaction_strategy::get_buckets(boost::copy_range<std::vector<sstable_run>>(_all | boost::adaptors::map_values), _options)) {
            if (!incremental_compaction_strategy::is_bucket_interesting(bucket, _threshold)) {
                continue;
            }
            for (const sstable_run& run : bucket) {
                auto data_size = run.data_size();
                if (data_size > 0) {
                    _total_backlog_bytes += data_size;
                    _sstables_backlog_contribution += data_size * log4(data_size);
                    _sstable_runs_contributing_backlog.insert((*run.all().begin())->run_identifier());
                }
            }
        }
    }
public:
    incremental_backlog_tracker(incremental_compaction_strategy_options options) : _options(std::move(options)) {}

    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        inflight_component compacted = compacted_backlog(oc);

        // Bail out if effective backlog is zero
        if (_total_backlog_bytes <= compacted.total_bytes) {
            return 0;
        }

        // Formula for each SSTable is (Si - Ci) * log(T / Si)
        // Which can be rewritten as: ((Si - Ci) * log(T)) - ((Si - Ci) * log(Si))
        //
        // For the meaning of each variable, please refer to the doc in size_tiered_backlog_tracker.hh

        // Sum of (Si - Ci) for all SSTables contributing backlog
        auto effective_backlog_bytes = _total_backlog_bytes - compacted.total_bytes;

        // Sum of (Si - Ci) * log (Si) for all SSTables contributing backlog
        auto sstables_contribution = _sstables_backlog_contribution - compacted.contribution;
        // This is subtracting ((Si - Ci) * log (Si)) from ((Si - Ci) * log(T)), yielding the final backlog
        auto b = (effective_backlog_bytes * log4(_total_bytes)) - sstables_contribution;
        return b > 0 ? b : 0;
    }

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    virtual void replace_sstables(std::vector<sstables::shared_sstable> old_ssts, std::vector<sstables::shared_sstable> new_ssts) override {
      for (auto&& sst : new_ssts) {
        if (sst->data_size() > 0) {
            _all[sst->run_identifier()].insert(sst);
            _total_bytes += sst->data_size();
            // Deduce threshold from the last SSTable added to the set
            _threshold = sst->get_schema()->min_compaction_threshold();
        }
      }

      bool exhausted_input_run = false;
      for (auto&& sst : old_ssts) {
        if (sst->data_size() > 0) {
            auto run_identifier = sst->run_identifier();
            _all[run_identifier].erase(sst);
            if (_all[run_identifier].all().empty()) {
                _all.erase(run_identifier);
                exhausted_input_run = true;
            }
            _total_bytes -= sst->data_size();
        }
      }
      // Backlog contribution will only be refreshed when an input SSTable run was exhausted by
      // compaction, so to avoid doing it for each exhausted fragment, which would be both
      // overkill and expensive.
      if (exhausted_input_run) {
          refresh_sstables_backlog_contribution();
      }
    }
    int64_t total_bytes() const {
        return _total_bytes;
    }
};
