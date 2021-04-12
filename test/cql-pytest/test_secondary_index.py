# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

# Tests for secondary indexes

import time
import pytest
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure

from util import new_test_table

# A reproducer for issue #7443: Normally, when the entire table is SELECTed,
# the partitions are returned sorted by the partitions' token. When there
# is filtering, this order is not expected to change. Furthermore, when this
# filtering happens to use a secondary index, again the order is not expected
# to change.
def test_partition_order_with_si(cql, test_keyspace):
    schema = 'pk int, x int, PRIMARY KEY ((pk))'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Insert 20 partitions, all of them with x=1 so that filtering by x=1
        # will yield the same 20 partitions:
        N = 20
        stmt = cql.prepare('INSERT INTO '+table+' (pk, x) VALUES (?, ?)')
        for i in range(N):
            cql.execute(stmt, [i, 1])
        # SELECT all the rows, and verify they are returned in increasing
        # partition token order (note that the token is a *signed* number):
        tokens = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table)]
        assert len(tokens) == N
        assert sorted(tokens) == tokens
        # Now select all the partitions with filtering of x=1. Since all
        # rows have x=1, this shouldn't change the list of matching rows, and
        # also shouldn't check their order:
        tokens1 = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table+' WHERE x=1 ALLOW FILTERING')]
        assert tokens1 == tokens
        # Now add an index on x, which allows implementing the "x=1"
        # restriction differently. With the index, "ALLOW FILTERING" is
        # no longer necessary. But the order of the results should
        # still not change. Issue #7443 is about the order changing here.
        cql.execute('CREATE INDEX ON '+table+'(x)')
        # "CREATE INDEX" does not wait until the index is actually available
        # for use. Reads immediately after the CREATE INDEX may fail or return
        # partial results. So let's retry until reads resume working:
        for i in range(100):
            try:
                tokens2 = [row.system_token_pk for row in cql.execute('SELECT token(pk) FROM '+table+' WHERE x=1')]
                if len(tokens2) == N:
                    break
            except ReadFailure:
                pass
            time.sleep(0.1)
        assert tokens2 == tokens

# Test which ensures that indexes for a query are picked by the order in which
# they appear in restrictions. That way, users can deterministically pick
# which indexes are used for which queries.
# Note that the order of picking indexing is not set in stone and may be
# subject to change - in which case this test case should be amended as well.
# The order tested in this case was decided as a good first step in issue
# #7969, but it's possible that it will eventually be implemented another
# way, e.g. dynamically based on estimated query selectivity statistics.
# Ref: #7969
@pytest.mark.xfail(reason="The order of picking indexes is currently arbitrary. Issue #7969")
def test_order_of_indexes(scylla_only, cql, test_keyspace):
    schema = 'p int primary key, v1 int, v2 int, v3 int'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE INDEX my_v3_idx ON {table}(v3)")
        cql.execute(f"CREATE INDEX my_v1_idx ON {table}(v1)")
        cql.execute(f"CREATE INDEX my_v2_idx ON {table}((p),v2)")
        # All queries below should use the first index they find in the list
        # of restrictions. Tracing information will be consulted to ensure
        # it's true. Currently some of the cases below succeed, because the
        # order is not well defined (and may, for instance, change upon
        # server restart), but some of them fail. Once a proper ordering
        # is implemented, all cases below should succeed.
        def index_used(query, index_name):
            assert any([index_name in event.description for event in cql.execute(query, trace=True).get_query_trace().events])
        index_used(f"SELECT * FROM {table} WHERE v3 = 1", "my_v3_idx")
        index_used(f"SELECT * FROM {table} WHERE v3 = 1 and v1 = 2 allow filtering", "my_v3_idx")
        index_used(f"SELECT * FROM {table} WHERE p = 1 and v1 = 1 and v3 = 2 allow filtering", "my_v1_idx")
        index_used(f"SELECT * FROM {table} WHERE p = 1 and v3 = 1 and v1 = 2 allow filtering", "my_v3_idx")
        # Local indexes are still skipped if they cannot be used
        index_used(f"SELECT * FROM {table} WHERE v2 = 1 and v1 = 2 allow filtering", "my_v1_idx")
        index_used(f"SELECT * FROM {table} WHERE v2 = 1 and v3 = 2 and v1 = 3 allow filtering", "my_v3_idx")
        index_used(f"SELECT * FROM {table} WHERE v1 = 1 and v2 = 2 and v3 = 3 allow filtering", "my_v1_idx")
        # Local indexes are still preferred over global ones, if they can be used
        index_used(f"SELECT * FROM {table} WHERE p = 1 and v1 = 1 and v3 = 2 and v2 = 2 allow filtering", "my_v2_idx")
        index_used(f"SELECT * FROM {table} WHERE p = 1 and v2 = 1 and v1 = 2 allow filtering", "my_v2_idx")
