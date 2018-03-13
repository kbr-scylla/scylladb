/*
 * Copyright 2016 ScyllaDB
 */
/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include "priority_manager.hh"

namespace service {
priority_manager& get_local_priority_manager() {
    static thread_local priority_manager pm = priority_manager();
    return pm;
}
}
