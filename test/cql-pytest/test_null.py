# Copyright 2020 ScyllaDB
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
from util import unique_name, random_string


@pytest.fixture(scope="session")
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
@pytest.mark.xfail(reason="issue #7852")
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
