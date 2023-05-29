import logging
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.topology.util import wait_for_token_ring_and_group0_consistency
import pytest
from pytest import FixtureRequest


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_nodes_with_different_smp(manager: ManagerClient) -> None:
    config = {
        'consistent_cluster_management': False,
        #'skip_wait_for_gossip_to_settle': 1,
    }
    await manager.server_add(config=config)
    config = config | {
        'error_injections_at_startup': ['sync_pull']
    }
    await manager.server_add(config=config)
