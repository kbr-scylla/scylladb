import logging
import time
import pytest
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.rest_client import inject_error


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_schema_agreement_timeout(manager: ManagerClient) -> None:
    s1 = await manager.server_add()
    await manager.server_add()
    s3 = await manager.server_add()

    cql = manager.cql
    assert(cql)

    h1, h3 = await wait_for_cql_and_get_hosts(cql, [s1, s3], time.time() + 60)

    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    async with inject_error(manager.api, s3.ip_addr, 'slow_apply'):
        for i in range(60):
            start = time.time()
            await cql.run_async(f"create table ks.t{i} (pk int primary key)", host=h1)
            end = time.time()
            dur = end - start
            logger.info(f"Took {dur}")
            await cql.run_async(f"select * from ks.t{i}", host=h3)
