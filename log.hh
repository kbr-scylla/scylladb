/*
 * Copyright (C) 2014 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/util/log.hh>

namespace logging {

//
// Seastar changed the names of some of these types. Maintain the old names here to avoid too much churn.
//

using log_level = seastar::log_level;
using logger = seastar::logger;
using registry = seastar::logger_registry;

inline registry& logger_registry() noexcept {
    return seastar::global_logger_registry();
}

using settings = seastar::logging_settings;

inline void apply_settings(const settings& s) {
    seastar::apply_logging_settings(s);
}

using seastar::pretty_type_name;
using seastar::level_name;

}
