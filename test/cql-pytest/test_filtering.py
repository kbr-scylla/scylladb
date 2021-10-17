# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

#############################################################################
# Tests for the SELECT requests with various filtering expressions.
# We have a separate test file test_allow_filtering.py, for tests that
# focus on whether the string "ALLOW FILTERING" is needed or not needed
# for a query. In the tests in this file we focus more on the correctness
# of various filtering expressions - regardless of whether ALLOW FILTERING
# is or isn't necessary.

import pytest
from util import new_test_table
from cassandra.protocol import InvalidRequest
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION

# When filtering for "x > 0" or "x < 0", rows with an unset value for x
# should not match the filter.
# Reproduces issue #6295 and its duplicate #8122.
def test_filter_on_unset(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        cql.execute(f"INSERT INTO {table} (a) VALUES (1)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (2, 2)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (3, -1)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b>0 ALLOW FILTERING")) == [(2,)]
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b<0 ALLOW FILTERING")) == [(3,)]
        cql.execute(f"ALTER TABLE {table} ADD c int")
        cql.execute(f"INSERT INTO {table} (a, b,c ) VALUES (4, 5, 6)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c<0 ALLOW FILTERING")) == []
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c>0 ALLOW FILTERING")) == [(4,)]

# Reproducers for issue #8203, which test a scan (whole-table or single-
# partition) with filtering which keeps just the last row, after a long list
# of non-matching rows.
# As usual, the scan is done with paging, and since most rows do not match
# the filter, several empty pages should be returned until finally we get
# the expected matching row. If we allow the Python driver to iterate over
# all results, it should read all these pages and give us the one result.
# The bug is that the iteration stops prematurely (it seems after the second
# empty page) and an empty result set is returned.
# It turns out that this was a bug in the Python driver, not in Scylla,
# which was fixed by
# https://github.com/datastax/python-driver/commit/1d9077d3f4c937929acc14f45c7693e76dde39a9
# So below we check if the driver version is recent enough (the Datastax
# and Scylla versions have different version numbers), and if not, we skip
# this test.

# Reproducer for issue #8203, partition-range (whole-table) scan case
def test_filtering_contiguous_nonmatching_partition_range(cql, test_keyspace):
    # Verify that the Python driver is recent enough to contain the fix for
    # the driver bug that caused this issue. If it's too old, this test cannot
    # succeed, and we just skip it. Scylla drivers 3.24.5 or newer contain
    # this fix, Datastax drivers 3.25.1 or newer contain it. The fix was
    # introduced in the following commits:
    # https://github.com/scylladb/python-driver/commit/6ed53d9f7004177e18d9f2ea000a7d159ff9278e,
    # https://github.com/datastax/python-driver/commit/1d9077d3f4c937929acc14f45c7693e76dde39a9
    scylla_driver = 'Scylla' in DRIVER_NAME
    driver_version = tuple(int(x) for x in DRIVER_VERSION.split('.'))
    if (scylla_driver and driver_version < (3, 24, 5) or
            not scylla_driver and driver_version <= (3, 25, 0)):
        pytest.skip("Python driver too old to run this test")
    # The bug depends on the amount of data being scanned passing some
    # page size limit, so it doesn't matter if the reproducer has a lot of
    # small rows or fewer long rows - and inserting fewer long rows is
    # significantly faster.
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v) VALUES (?, '{long}', ?)")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # We want the filter to match only at the end the scan - but we don't
        # know the partition order (we don't want the test to depend on the
        # partitioner). So we first figure out a partition near the end (at
        # some high token), and use that in the filter.
        p, v = list(cql.execute(f"SELECT p, v FROM {table} WHERE TOKEN(p) > 8000000000000000000 LIMIT 1"))[0]
        assert list(cql.execute(f"SELECT p FROM {table} WHERE v={v} ALLOW FILTERING")) == [(p,)]

# Reproducer for issue #8203, single-partition scan case
def test_filtering_contiguous_nonmatching_single_partition(cql, test_keyspace):
    scylla_driver = 'Scylla' in DRIVER_NAME
    driver_version = tuple(int(x) for x in DRIVER_VERSION.split('.'))
    if (scylla_driver and driver_version < (3, 24, 5) or
            not scylla_driver and driver_version <= (3, 25, 0)):
        pytest.skip("Python driver too old to run this test")
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c int, s text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v, s) VALUES (1, ?, ?, '{long}')")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # To fail this test, we must select s here. If s is not selected,
        # Scylla won't count its size as part of the 1MB limit, and will not
        # return empty pages - the first page will contain the result.
        assert list(cql.execute(f"SELECT c, s FROM {table} WHERE p=1 AND v={count-1} ALLOW FILTERING")) == [(count-1, long)]

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        yield table

# Although the "!=" operator exists in the parser and might be allowed in
# other places (e.g., LWT), it is *NOT* supported in WHERE clauses - not
# for filtering, and also not in relations tha don't need filtering
# (on partition keys or tokens). It is not supported in either Cassandra or
# Scylla, and there are no plans to add this support, so for now the test
# verifies that at least we get the expected error.
def test_operator_ne_not_supported(cql, table1):
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE b != 0 ALLOW FILTERING')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE a != 0')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE token(a) != 0')
