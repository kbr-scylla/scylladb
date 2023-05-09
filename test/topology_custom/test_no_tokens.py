import pytest
import logging
import time
import asyncio

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_no_tokens(manager: ManagerClient) -> None:
    s1 = await manager.server_add()
    s2 = await manager.server_add()

    await asyncio.sleep(1)

    async with inject_error(manager.api, s2.ip_addr, 'hsn_sleep'):
        s3 = await manager.server_add()

    await manager.server_stop_gracefully(s3.server_id)

    await asyncio.sleep(1)
    await manager.api.client.get_json('/storage_service/host_id', s2.ip_addr)
