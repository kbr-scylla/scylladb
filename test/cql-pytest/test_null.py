# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

#############################################################################
# Tests for finer points of the meaning of "null" in various places
#############################################################################

import pytest
import re
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure
from util import unique_name, random_string, new_test_table


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p text, c text, v text, primary key (p, c))")
    yield table
    cql.execute("DROP TABLE " + table)

# An item cannot be inserted without a key. Verify that before we get into
# the really interesting test below - trying to pass "null" as the value of
# the key.
# See also issue #3665.
def test_insert_missing_key(cql, table1):
    s = random_string()
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
    s = random_string()
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

def test_primary_key_in_null(cql, table1):
    '''Tests handling of "key_column in ?" where ? is bound to null.'''
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p IN ?"), [None])
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p='' AND c IN ?"), [None])
    with pytest.raises(InvalidRequest, match='Invalid null value for IN restriction'):
        cql.execute(cql.prepare(f"SELECT p FROM {table1} WHERE p='' AND (c) IN ?"), [None])

# Cassandra says "IN predicates on non-primary-key columns (v) is not yet supported".
def test_regular_column_in_null(scylla_only, cql, table1):
    '''Tests handling of "regular_column in ?" where ? is bound to null.'''
    # Without any rows in the table, SELECT will shortcircuit before evaluating the WHERE clause.
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('p', 'c')")
    with pytest.raises(InvalidRequest, match='null value'):
        cql.execute(cql.prepare(f"SELECT v FROM {table1} WHERE v IN ? ALLOW FILTERING"), [None])

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
    p = random_string()
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{p}', '1', 'hello')")
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{p}', '2', '')")
    cql.execute(f"INSERT INTO {table1} (p,c) VALUES ('{p}', '3')")
    # As explained above, none of the above-inserted rows should match -
    # not even the one with an unset v:
    assert list(cql.execute(f"SELECT c FROM {table1} WHERE p='{p}' AND v=NULL ALLOW FILTERING")) == []

# In test_insert_null_key() above we verified that a null value is not
# allowed as a key column - neither as a partition key nor clustering key.
# An *empty string*, in contrast, is NOT a null. So ideally should have been
# allowed as a key. However, for undocumented reasons (having to do with how
# partition keys are serialized in sstables), an empty string is NOT allowed
# as a partition key. It is allowed as a clustering key, though. In the
# following test we confirm those things.
# See issue #9352.
def test_insert_empty_string_key(cql, table1):
    s = random_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('{s}', '', 'cat')")
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c=''")) == [('cat',)]
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"INSERT INTO {table1} (p,c,v) VALUES ('', '{s}', 'dog')")

# test_update_empty_string_key() is the same as test_insert_empty_string_key()
# just uses an UPDATE instead of INSERT. It turns out that exactly the cases
# which are allowed by INSERT are also allowed by UPDATE.
def test_update_empty_string_key(cql, table1):
    s = random_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"UPDATE {table1} SET v = 'cat' WHERE p='{s}' AND c=''")
    assert list(cql.execute(f"SELECT v FROM {table1} WHERE p='{s}' AND c=''")) == [('cat',)]
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"UPDATE {table1} SET v = 'dog' WHERE p='' AND c='{s}'")

# ... and same for DELETE
def test_delete_empty_string_key(cql, table1):
    s = random_string()
    # An empty-string clustering *is* allowed:
    cql.execute(f"DELETE FROM {table1} WHERE p='{s}' AND c=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"DELETE FROM {table1} WHERE p='' AND c='{s}'")


# Although an empty string is not allowed as a partition key (as tested
# above by test_empty_string_key()), it turns out that in a *compound*
# partition key (with multiple partition-key columns), any or all of them
# may be empty strings! This inconsistency is known in Cassandra, but
# deemed unworthy to fix - see:
#    https://issues.apache.org/jira/browse/CASSANDRA-11487
def test_empty_string_key2(cql, test_keyspace):
    schema = 'p1 text, p2 text, c text, v text, primary key ((p1, p2), c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        s = random_string()
        cql.execute(f"INSERT INTO {table} (p1,p2,c,v) VALUES ('', '', '', 'cat')")
        cql.execute(f"INSERT INTO {table} (p1,p2,c,v) VALUES ('x', 'y', 'z', 'dog')")
        assert list(cql.execute(f"SELECT v FROM {table} WHERE p1='' AND p2='' AND c=''")) == [('cat',)]
