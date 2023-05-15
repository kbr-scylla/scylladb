import pytest
from test.pylib.manager_client import ManagerClient


@pytest.mark.asyncio
async def test_banned_node(manager: ManagerClient) -> None:
    srv = await manager.server_add()
    await manager.server_pause(srv.server_id)
