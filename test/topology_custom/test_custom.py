#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

from cassandra import ConsistencyLevel # type: ignore
from cassandra.query import SimpleStatement # type: ignore

import pytest
import datetime
import time
import logging


@pytest.mark.asyncio
async def test_cdc_streams_rewrite(request, manager: ManagerClient):
    srv = await manager.server_add()
    await manager.driver_connect()
    cql = manager.cql
    assert(cql)
    await cql.run_async("""
        CREATE TABLE system_distributed.cdc_generation_descriptions (
            time timestamp PRIMARY KEY,
            description frozen<list<frozen<tuple<bigint, frozen<list<blob>>, tinyint>>>>,
            expired timestamp
        )""")
    await cql.run_async("""
        CREATE TABLE system_distributed.cdc_streams_descriptions (
            time timestamp PRIMARY KEY,
            streams frozen<set<blob>>,
            expired timestamp
        )""")
    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    await cql.run_async("create table ks.t (pk int primary key) with cdc = {'enabled': true};")

    tz = datetime.timezone.utc
    now = datetime.datetime.now(tz)
    ts = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0000"

    await cql.run_async(SimpleStatement(
         "insert into system_distributed.cdc_generation_descriptions (time, description)"
        f" values ('{ts}', [(10, [0xe6900000000000000630df9c40000621], 12)]);",
        consistency_level = ConsistencyLevel.ONE))
    await cql.run_async(SimpleStatement(
         "insert into system_distributed.cdc_streams_descriptions (time, streams)"
        f" values ('{ts}', {{0xe6900000000000000630df9c40000621}});",
        consistency_level = ConsistencyLevel.ONE))
    await cql.run_async("delete from system.cdc_local where key = 'rewritten'")

    await manager.server_restart(srv.server_id)
    manager.driver_close()
    await manager.driver_connect()
    cql = manager.cql
    assert(cql)

    await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 60)
    async def rewritten():
        rs = await cql.run_async("select key from system.cdc_local where key = 'rewritten'")
        if len(rs) > 0:
            return True
    await wait_for(rewritten, time.time() + 60)

    ts = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+0000"
    timestamps = await cql.run_async(SimpleStatement(
         "select time from system_distributed.cdc_generation_timestamps"
        f" where key = 'timestamps' and time = '{ts}'",
        consistency_level = ConsistencyLevel.ONE))
    logging.info(f"timestamps: {timestamps}")
    assert(len(timestamps) == 1)

    streams = await cql.run_async(SimpleStatement(
         "select streams from system_distributed.cdc_streams_descriptions_v2"
        f" where time = '{ts}'",
        consistency_level = ConsistencyLevel.ONE))
    logging.info(f"streams: {streams}")
    assert(len(streams) == 1)
    streams = streams[0].streams
    logging.info(f"streams: {streams}")
    assert(len(streams) == 1)
    assert(next(iter(streams)) == bytes.fromhex('e6900000000000000630df9c40000621'))
