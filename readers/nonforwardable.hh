/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

class flat_mutation_reader_v2;

flat_mutation_reader_v2 make_nonforwardable(flat_mutation_reader_v2, bool);
