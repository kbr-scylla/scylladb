from test.pylib.manager_client import ManagerClient
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_concurrent_tasks(manager: ManagerClient):
    srv = await manager.server_add()
    await manager.server_stop(srv.server_id)

    async def fail():
        await asyncio.sleep(0.01)
        pytest.fail()

    start_task = asyncio.create_task(manager.server_start(srv.server_id))
    fail_task = asyncio.create_task(fail())

    await asyncio.gather(*[start_task, fail_task])
