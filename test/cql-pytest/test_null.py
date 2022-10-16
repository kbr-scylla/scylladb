# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary

#############################################################################
# Tests for finer points of the meaning of "null" in various places
#############################################################################

import pytest
import re
from cassandra.protocol import InvalidRequest
from util import unique_name, unique_key_string


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p text, c text, v text, i int, s set<int>, m map<int, int>, primary key (p, c))")
    yield table
    cql.execute("DROP TABLE " + table)

# An item cannot be inserted without a key. Verify that before we get into
# the really interesting test below - trying to pass "null" as the value of
# the key.
# See also issue #3665.
def test_insert_missing_key(cql, table1):
    s = unique_key_string()
    # A clustering key is missing. Cassandra uses the message "Some clustering
    # keys are missing: c", and Scylla: "Missing mandatory PRIMARY KEY part c"
    with pytest.raises(InvalidRequest, match=re.compile('missing', re.IGNORECASE)):
        cql.execute(f"INSERT INTO {table1} (p) VALUES ('{s}')")
    # Similarly, a missing partition key
    with pytest.raises(InvalidRequest, match=re.compile('missing', re.IGNORECASE)):
        cql.execute(f"INSERT INTO {table1} (c) VALUES ('{s}')")

# A null key, like a missing one, is also not allowed.
# This reproduces issue #7852.
def test_insert_null_key(cql, table1):
    s = unique_key_string()
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{s}', null)")
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(f"INSERT INTO {table1} (p,c) VALUES (null, '{s}')")
    # Try the same thing with prepared statement, where a "None" stands for
    # a null. Note that this is completely different from UNSET_VALUE - only
    # with the latter should the insertion be ignored.
    stmt = cql.prepare(f"INSERT INTO {table1} (p,c) VALUES (?, ?)")
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(stmt, [s, None])
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(stmt, [None, s])

# Tests handling of "key_column in ?" where ? is bound to null.
# Reproduces issue #8265.
def test_primary_key_in_null(scylla_only, cql, table1):
    assert list(cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p IN ?"), [None])) == []
    assert list(cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p='' AND c IN ?"), [None])) == []
    assert list(cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p='' AND (c) IN ?"), [None])) == []

# Cassandra says "IN predicates on non-primary-key columns (v) is not yet supported".
def test_regular_column_in_null(scylla_only, cql, table1):
    '''Tests handling of "regular_column in ?" where ? is bound to null.'''
    # Without any rows in the table, SELECT will shortcircuit before evaluating the WHERE clause.
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('p', 'c')")
    assert list(cql.execute(cql.prepare(f"SELECT v FROM {table1} WHERE v IN ? ALLOW FILTERING"), [None])) == []

# Though nonsensical, this operation is allowed by Cassandra.  Ensure we allow it, too.
def test_delete_impossible_clustering_range(cql, table1):
    cql.execute(f"DELETE FROM {table1} WHERE p='p' and c<'a' and c>'a'")

def test_delete_null_key(cql, table1):
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(f"DELETE FROM {table1} WHERE p=null")
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(cql.prepare(f"DELETE FROM {table1} WHERE p=?"), [None])
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(f"DELETE FROM {table1} WHERE p='p' AND c=null")
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(cql.prepare(f"DELETE FROM {table1} WHERE p='p' AND c=?"), [None])

# Test what SELECT does with the restriction "WHERE v=NULL".
# In SQL, "WHERE v=NULL" doesn't match anything - because nothing is equal
# to null - not even null. SQL also provides a more useful restriction
# "WHERE v IS NULL" which matches all rows where v is unset.
# Scylla and Cassandra do *not* support the "IS NULL" syntax yet (they do
# have "IS NOT NULL" but only in a definition of a materialized view),
# so it is commonly requested that "WHERE v=NULL" should do what "IS NULL"
# is supposed to do - see issues #4776 and #8489 for Scylla and
# CASSANDRA-10715 for Cassandra, where this feature was requested.
# Nevertheless, in Scylla we decided to follow SQL: "WHERE v=NULL" should
# matche nothing, not even rows where v is unset. This is what the following
# test verifies.
# This test fails on Cassandra (hence cassandra_bug) because Cassandra
# refuses the "WHERE v=NULL" relation, rather than matching nothing.
# We consider this a mistake, and not something we want to emulate in Scylla.
def test_filtering_eq_null(cassandra_bug, cql, table1):
    p = unique_key_string()
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{p}', '1', 'hello')")
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{p}', '2', '')")
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
    # As explained above, none of the above-inserted rows should match -
    # not even the one with an unset v:
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND v=NULL ALLOW FILTERING")) == []

# Similarly, inequality restrictions with NULL, like > NULL, also match
# nothing.
def test_filtering_inequality_null(cassandra_bug, cql, table1):
    p = unique_key_string()
    cql.execute(f"INSERT INTO {table1} (p,c,i) VALUES ('{p}', '1', 7)")
    cql.execute(f"INSERT INTO {table1} (p,c,i) VALUES ('{p}', '2', -3)")
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND i>NULL ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND i>=NULL ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND i<NULL ALLOW FILTERING")) == []
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND i<=NULL ALLOW FILTERING")) == []

# Similarly, CONTAINS restriction with NULL should also match nothing.
# Reproduces #10359.
def test_filtering_contains_null(cassandra_bug, cql, table1):
    p = unique_key_string()
    cql.execute(f"INSERT INTO {table1} (p,c,s) VALUES ('{p}', '1', {{1, 2}})")
    cql.execute(f"INSERT INTO {table1} (p,c,s) VALUES ('{p}', '2', {{3, 4}})")
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND s CONTAINS NULL ALLOW FILTERING")) == []

# Similarly, CONTAINS KEY restriction with NULL should also match nothing.
# Reproduces #10359.
def test_filtering_contains_key_null(cassandra_bug, cql, table1):
    p = unique_key_string()
    cql.execute(f"INSERT INTO {table1} (p,c,m) VALUES ('{p}', '1', {{1: 2}})")
    cql.execute(f"INSERT INTO {table1} (p,c,m) VALUES ('{p}', '2', {{3: 4}})")
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND m CONTAINS KEY NULL ALLOW FILTERING")) == []

# The above tests test_filtering_eq_null and test_filtering_inequality_null
# have WHERE x=NULL or x>NULL where "x" is a regular column. Such a
# comparison requires ALLOW FILTERING for non-NULL parameters, so we also
# require it for NULL. Unlike the previous tests, this one also passed on
# Cassandra.
def test_filtering_null_comparison_no_filtering(cql, table1):
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND i=NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND i>NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND i>=NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND i<NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND i<=NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND s CONTAINS NULL")
    with pytest.raises(InvalidRequest, match='ALLOW FILTERING'):
        cql.execute(f"SELECT c FROM {table1} WHERE p='x' AND m CONTAINS KEY NULL")

# Cassandra considers the null subscript 'm[null]' to be an invalid request.
# In Scylla we decided to it differently (we think better): m[null] is simply
# a null, so the filter 'WHERE m[null] = 3' is not an error - it just doesn't
# match anything. This is more consistent with our usual null handling (null[2]
# and null < 2 are both defined as returning null), and will also allow us
# in the future to support non-constant subscript - for example m[a] where
# the column a can be null for some rows and non-null for other rows.
# Before we implemented the above decision, we had multiple bugs in this case,
# resulting in bizarre errors and even crashes (see #10361, #10399 and #10417).
#
# Because this test uses a shared table (table1), then depending on how it's
# run, it sometimes sees an empty table and sometimes a table with data
# (and null values for the map m...), so this test mixes several different
# concerns and problems. The same problems are better covered separately
# by test_filtering.py::test_filtering_with_subscript and
# test_filtering.py::test_filtering_null_map_with_subscript so this test
# should eventually be deleted.
def test_map_subscript_null(cql, table1, cassandra_bug):
    assert list(cql.execute(f"SELECT p FROM {table1} WHERE m[null] = 3 ALLOW FILTERING")) == []
    assert list(cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE m[?] = 3 ALLOW FILTERING"), [None])) == []
