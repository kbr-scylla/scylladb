# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.

#############################################################################
# Tests for user-defined aggregates (UDA)
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from util import unique_name, new_test_table, new_function, new_aggregate

# Test that computing an average by hand works the same as
# the built-in function
def test_custom_avg(scylla_only, cql, test_keyspace):
    schema = 'id bigint primary key'
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(8):
            cql.execute(f"INSERT INTO {table} (id) VALUES ({10**i})")
        avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
        div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
        with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun:
            custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
            with new_aggregate(cql, test_keyspace, custom_avg_body) as custom_avg:
                custom_res = [row for row in cql.execute(f"SELECT {test_keyspace}.{custom_avg}(id) AS result FROM {table}")]
                avg_res = [row for row in cql.execute(f"SELECT avg(id) AS result FROM {table}")]
                assert custom_res == avg_res

# Test that computing an aggregate which takes 2 parameters works fine.
# In this case - it's a simple map literal builder.
def test_map_literal_builder(scylla_only, cql, test_keyspace):
    schema = 'id int, k text, val int, primary key (id, k)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for i in range(8):
            cql.execute(f"INSERT INTO {table} (id, k, val) VALUES (0, '{chr(ord('a') + i)}', {i})")
        map_literal_partial_body = "(state text, id text, val int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE lua AS 'return state..id..\":\"..tostring(val)..\",\"'"
        finish_body = "(state text) CALLED ON NULL INPUT RETURNS text LANGUAGE lua AS 'return state..\"}\"'"
        with new_function(cql, test_keyspace, map_literal_partial_body) as map_literal_partial, new_function(cql, test_keyspace, finish_body) as finish_fun:
            map_literal_body = f"(text, int) SFUNC {map_literal_partial} STYPE text FINALFUNC {finish_fun} INITCOND '{{'"
            with new_aggregate(cql, test_keyspace, map_literal_body) as map_literal:
                map_res = [row for row in cql.execute(f"SELECT {test_keyspace}.{map_literal}(k, val) AS result FROM {table}")]
                assert len(map_res) == 1 and map_res[0].result == '{a:0,b:1,c:2,d:3,e:4,f:5,g:6,h:7,}'

# Test that the state function and final function must exist and have correct signatures
def test_wrong_sfunc_or_ffunc(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, text>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, text> LANGUAGE lua AS 'return {state[1] + val, \"hello\"}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    div_body2 = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS float LANGUAGE lua AS 'return state[1]/state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun, new_function(cql, test_keyspace, div_body2) as div_fun2:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun2} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC i_do_not_exist STYPE tuple<bigint, bigint> FINALFUNC {div_fun2} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC i_do_not_exist_either INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")

# Test that dropping the state function or the final function is not allowed if it's used by an aggregate
def test_drop_sfunc_or_ffunc(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, bigint>, val bigint) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with new_aggregate(cql, test_keyspace, custom_avg_body) as custom_avg:
            with pytest.raises(InvalidRequest, match="it is used"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{avg_partial}")
            with pytest.raises(InvalidRequest, match="it is used"):
                cql.execute(f"DROP FUNCTION {test_keyspace}.{div_fun}")

# Test that the state function takes a correct number of arguments - the state and the new input
def test_incorrect_state_func(scylla_only, cql, test_keyspace):
    avg_partial_body = "(state tuple<bigint, bigint>, val bigint, redundant int) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + val, state[2] + 1}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg_partial_body) as avg_partial, new_function(cql, test_keyspace, div_body) as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="State function not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")
    avg2_partial_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS tuple<bigint, bigint> LANGUAGE lua AS 'return {state[1] + 42, state[2] + 1}'"
    div_body = "(state tuple<bigint, bigint>) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return state[1]//state[2]'"
    with new_function(cql, test_keyspace, avg2_partial_body) as avg2_partial, new_function(cql, test_keyspace, div_body) as div_fun:
        custom_avg_body = f"(bigint) SFUNC {avg2_partial} STYPE tuple<bigint, bigint> FINALFUNC {div_fun} INITCOND (0,0)"
        with pytest.raises(InvalidRequest, match="State function not found"):
            cql.execute(f"CREATE AGGREGATE {test_keyspace}.{unique_name()} {custom_avg_body}")