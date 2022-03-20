/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

class flat_mutation_reader;

flat_mutation_reader make_nonforwardable(flat_mutation_reader, bool);

