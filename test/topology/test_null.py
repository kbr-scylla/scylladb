#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
from cassandra.protocol import InvalidRequest   # type: ignore
from pylib.util import unique_name
import pytest


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p text, c text, v text, primary key (p, c))")
    yield table
    cql.execute("DROP TABLE " + table)


def test_delete_empty_string_key(cql, table1):
    s = "foobar"
    # An empty-string clustering *is* allowed:
    cql.execute(f"DELETE FROM {table1} WHERE p='{s}' AND c=''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        cql.execute(f"DELETE FROM {table1} WHERE p='' AND c='{s}'")
