import logging
import asyncio
import pytest

from test.pylib.manager_client import ManagerClient

@pytest.mark.asyncio
async def test_double_execution(request, manager: ManagerClient):
    await manager.server_add()
    await manager.servers_add(2)

    logging.info(f'SLEEP 1')
    await asyncio.sleep(1)

    cql = manager.get_cql()
    hosts = cql.cluster.metadata.all_hosts()
    logging.info(f"hosts: {hosts}")

    logging.info(f'create ks')
    await cql.run_async("create keyspace ks with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
