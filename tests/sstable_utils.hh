/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include "sstables/sstables.hh"

sstables::shared_sstable make_sstable_containing(std::function<sstables::shared_sstable()> sst_factory, std::vector<mutation> muts);
