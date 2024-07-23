#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import logging
import time
import pytest
import asyncio

from cassandra.query import SimpleStatement
from cassandra.cluster import ConsistencyLevel

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import read_barrier, wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_udt_race(manager: ManagerClient) -> None:
    config = {'consistent_cluster_management': False,
              'enable_user_defined_functions': False,
              'experimental_features': [],
              }
    s1 = await manager.server_add(config=config)
    s2 = await manager.server_add(config=config)
    cql = manager.get_cql()

    logger.info("ks")
    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    logger.info("udt")
    await cql.run_async("create type ks.typ (a int)")
    logger.info("typ")
    await cql.run_async("create table ks.t (pk int primary key, x typ)")

    cql.cluster.max_schema_agreement_wait = False

    h1, h2 = [(await wait_for_cql_and_get_hosts(cql, [s], time.time() + 60))[0] for s in [s1, s2]]

    stopped = False
    ins = 0
    exc = 0
    async def task():
        nonlocal ins
        nonlocal exc
        while not stopped:
            try:
                await cql.run_async(SimpleStatement("insert into ks.t (pk, x) values (0, {a: 0, b: 0})", consistency_level=ConsistencyLevel.TWO), host=h2);
            except Exception as e:
                logger.info(f"exception: {e}")
                exc += 1
            else:
                ins += 1
            asyncio.sleep(0.01)

    ts = [asyncio.create_task(task()) for _ in range(2)]
    logger.info("sleep")
    await asyncio.sleep(0.05)
    logger.info("alter")
    await cql.run_async("alter type ks.typ add b int", host=h2);

    stopped = True
    logger.info("await")
    for t in ts:
        await t

    logger.info(f"ins {ins} exc {exc}")
