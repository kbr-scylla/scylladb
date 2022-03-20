# -*- coding: utf-8 -*-
# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary

#############################################################################
# Tests for batch operations
#############################################################################

from util import unique_name

import pytest


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k int primary key, t text)")
    yield table
    cql.execute("DROP TABLE " + table)


@pytest.mark.xfail(reason="Scylla does not send warnings for batch: https://github.com/scylladb/scylla/issues/10196")
def test_warnings_are_returned_in_response(cql, table1):
    """Verifies if response contains warning messages.
    Because warning message threshold is different for Scylla and Cassandra,
    test tries several sizes until warning appears.
    """
    for size_in_kb in [10, 129, 256]:
        statements = [f"INSERT INTO {table1} (k, t) VALUES ({idx}, '{'x' * 743}')" for idx in range(size_in_kb)]
        over_sized_batch = "BEGIN BATCH\n" + "\n".join(statements) + "\n APPLY BATCH\n"
        response_future = cql.execute_async(over_sized_batch)
        response_future.result()
        if response_future.warnings:
            break
    else:
        pytest.fail('Oversized batch did not generate a warning')

    # example message for Cassandra:
    # 'Batch for [cql_test_1647006065554.cql_test_1647006065623] is of size 7590,
    # exceeding specified threshold of 5120 by 2470.'
    assert "exceeding specified" in response_future.warnings[0]
