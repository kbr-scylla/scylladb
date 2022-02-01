/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "seastarx.hh"

class is_preemptible_tag;
using is_preemptible = bool_class<is_preemptible_tag>;
