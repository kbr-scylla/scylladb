# Copyright 2021 ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

#############################################################################
# Tests for min/max aggregate functions
#############################################################################

import pytest
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure
from cassandra.util import Date
from util import unique_name, random_string, new_test_table, project

# Regression-test for #7729.
def test_timeuuid(cql, test_keyspace):
    schema = "a int, b timeuuid, primary key (a,b)"
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f'insert into {table} (a, b) values (0, 13814000-1dd2-11ff-8080-808080808080)')
        cql.execute(f'insert into {table} (a, b) values (0, 6b1b3620-33fd-11eb-8080-808080808080)')
        assert project('system_todate_system_min_b',
                       cql.execute(f'select todate(min(b)) from {table} where a = 0')) == [Date('2020-12-01')]
        assert project('system_todate_system_max_b',
                       cql.execute(f'select todate(max(b)) from {table} where a = 0')) == [Date('2038-09-06')]
