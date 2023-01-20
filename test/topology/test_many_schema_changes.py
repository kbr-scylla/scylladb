import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables

@pytest.mark.asyncio
async def test_stuff(manager: ManagerClient, random_tables: RandomTables):
    for _ in range(40):
        await random_tables.add_table(ncolumns=5)
    await manager.server_add()
