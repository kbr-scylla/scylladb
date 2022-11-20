/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "compaction_strategy_impl.hh"
#include "compaction_strategy.hh"
#include "compaction.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>

namespace sstables {

class in_memory_backlog_tracker final : public compaction_backlog_tracker::impl {
public:
    double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return 0.5;
    }

    // Removing could be the result of a failure of an in progress write, successful finish of a
    // compaction, or some one-off operation, like drop
    void replace_sstables(std::vector<shared_sstable> old_sstables, std::vector<sstables::shared_sstable> new_sstables)  override {
    }
};

class in_memory_compaction_strategy_options {
public:
    in_memory_compaction_strategy_options(const std::map<sstring, sstring>& options) {}
    in_memory_compaction_strategy_options()  = default;
};

class in_memory_compaction_strategy : public compaction_strategy_impl {
    in_memory_compaction_strategy_options _options;

public:
    in_memory_compaction_strategy() = default;

    in_memory_compaction_strategy(const std::map<sstring, sstring>& options)
        : compaction_strategy_impl(options)
        , _options(options)
    {}

    explicit in_memory_compaction_strategy(const in_memory_compaction_strategy_options& options)
    : _options(options)
    {}

    compaction_descriptor get_sstables_for_compaction(table_state& cfs, strategy_control& control, std::vector<sstables::shared_sstable> candidates) override;

    int64_t estimated_pending_compactions(table_state& cf) const override;

    compaction_strategy_type type() const override {
        return compaction_strategy_type::in_memory;
    }

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() override {
        return std::make_unique<in_memory_backlog_tracker>();
    }
};

inline compaction_descriptor
in_memory_compaction_strategy::get_sstables_for_compaction(table_state& cfs, strategy_control& control, std::vector<sstables::shared_sstable> candidates) {
    // compact everything into one sstable
    if (candidates.size() > 1) {
        auto cd = sstables::compaction_descriptor(std::move(candidates), service::get_local_compaction_priority());
        cd.enable_garbage_collection(cfs.main_sstable_set());
        return cd;
    }
    return sstables::compaction_descriptor();
}

inline int64_t in_memory_compaction_strategy::estimated_pending_compactions(table_state& cf) const {
    return cf.main_sstable_set().all()->size() > 1;
}
}
