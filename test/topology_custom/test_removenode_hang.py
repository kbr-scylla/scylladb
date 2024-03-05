from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency

import time
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_removenode_hang(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'repair=trace']
    cfg = {'enable_user_defined_functions': False,
           'experimental_features': list[str]()}
    srvs = [await manager.server_add(cmdline=cmdline, config=cfg) for _ in range(5)]
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    await manager.server_stop_gracefully(srvs[-1].server_id)

    await manager.remove_node(srvs[0].server_id, srvs[-1].server_id)
