import pytest
import logging
import time

from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.rest_client import inject_error, inject_error_one_shot

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_new_table(request, manager) -> None:
    s1 = await manager.server_add()
    s2 = await manager.server_add()
    async with inject_error(manager.api, s2.ip_addr, 'update_tokens_sleep'):
        s3 = await manager.server_add()
        cql = manager.cql
        assert cql is not None
        h2 = (await wait_for_cql_and_get_hosts(cql, [s2], time.time() + 60))[0]
        await cql.run_async("create keyspace ks with replication"
                            " = {'class': 'SimpleStrategy', 'replication_factor': 1}", host=h2)
        await cql.run_async("create table ks.t (pk int primary key)", host=h2)
        logger.info(f"created table")
        peers = await cql.run_async("select peer from system.peers", host=h2)
        logger.info(f"peers: {peers}")
        h3 = (await wait_for_cql_and_get_hosts(cql, [s3], time.time() + 60))[0]
        peers = await cql.run_async("select peer from system.peers", host=h2)
        logger.info(f"peers: {peers}")
        await cql.run_async("insert into ks.t (pk) values (0)", host=h3)
        logger.info(f"inserted")
        peers = await cql.run_async("select peer from system.peers", host=h2)
        logger.info(f"peers: {peers}")
        res = await cql.run_async("select * from ks.t where pk = 0", host=h2)
        logger.info(f"res: {list(res)}")
        assert len(res) == 1
