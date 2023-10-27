#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test functionality on the cluster with different values of the --smp parameter on the nodes.
"""
import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest
import asyncio
from pytest import FixtureRequest

from cassandra.cluster import Session, ConsistencyLevel # type: ignore
from cassandra.query import SimpleStatement # type: ignore

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_alter_during_write(request: FixtureRequest, manager: ManagerClient) -> None:
    await manager.server_add(cmdline=['--logger-log-level', 'migration_manager=trace'])
    await manager.server_add(cmdline=['--logger-log-level', 'migration_manager=trace'])
    cql = manager.get_cql()

    logger.info(f"Creating keyspace and table")
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}")
    await cql.run_async(f"CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int)")

    finish_writes = await start_writes(cql)
    for i in range(20):
        logger.info(f"adding column it {i}")
        await cql.run_async("alter table ks.tbl add c int")
        logger.info(f"dropping column it {i}")
        await cql.run_async("alter table ks.tbl drop c")
    await finish_writes()


async def start_writes(cql: Session, concurrency: int = 3):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    stmt = SimpleStatement("INSERT INTO ks.tbl (pk, v) VALUES (0, 0)", consistency_level=ConsistencyLevel.TWO)

    async def do_writes(worker_id: int):
        write_count = 0
        last_error = None
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(stmt)
                write_count += 1
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                last_error = e
        logger.info(f"Worker #{worker_id} did {write_count} successful writes")
        if last_error is not None:
            raise last_error

    tasks = [asyncio.create_task(do_writes(worker_id)) for worker_id in range(concurrency)]

    async def finish():
        logger.info("Stopping write workers")
        stop_event.set()
        await asyncio.gather(*tasks)

    return finish
