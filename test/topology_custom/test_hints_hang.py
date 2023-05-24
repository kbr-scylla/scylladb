import asyncio
import logging
import time
import pytest

from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import ConsistencyLevel # type: ignore

from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_hints_hang(manager: ManagerClient) -> None:
    s1 = await manager.server_add()
    s2 = await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    cql = manager.get_cql()

    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await cql.run_async("create table ks.t (pk int primary key)")

    logger.info(f"stop {s2}")
    await manager.server_stop(s2.server_id)

    logger.info("insert")
    await cql.run_async(SimpleStatement("insert into ks.t (pk) values (0) using timeout 100ms",
                                        consistency_level=ConsistencyLevel.ONE))

    # Wait for the background part of the write to timeout, causing hint to get created
    # It will timeout after 100ms because of 'using timeout 100ms' in the insert above
    logger.info("sleep")
    await asyncio.sleep(1)

    # Wait until hints manager starts sending the hint (it happens every ~10 seconds)
    logger.info("sleep")
    await asyncio.sleep(10)

    logger.info(f"stop {s1} gracefully")
    await manager.server_stop_gracefully(s1.server_id)
