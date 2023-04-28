import asyncio
import pytest
import time
import logging
from pytest import FixtureRequest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.util import wait_for_token_ring_and_group0_consistency

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_schema_pull(request: FixtureRequest, manager: ManagerClient) -> None:
    srvs = [await manager.server_add(), await manager.server_add()]
    cql = manager.cql
    assert(cql)
    h1 = (await wait_for_cql_and_get_hosts(cql, [srvs[0]], time.time() + 60))[0]
    async def has_peer():
        peers = await cql.run_async("select * from system.peers", host=h1)
        if len(peers) > 0:
            return True
    logger.info("wait for peer")
    await wait_for(has_peer, time.time() + 60, period = 0.1)
    logger.info("wait for peer finished")

    #h2 = (await wait_for_cql_and_get_hosts(cql, [srvs[1]], time.time() + 60))[0]
    async with inject_error(manager.api, srvs[1].ip_addr, 'merge_keyspaces_sleep'):
        logger.info("creating ks")
        await cql.run_async(
                "create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                host=h1)
        logger.info("creating t")
        await cql.run_async(
                "create table ks.t (pk int primary key)",
                host=h1)
        logger.info("select")
        await cql.run_async(
                "select * from ks.t", trace=True,
                host=h1)
        logger.info("select finished")
        await asyncio.sleep(1)
