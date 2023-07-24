#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import pytest
import logging


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_restart_after_decom(request, manager: ManagerClient):
    logger.info("Bootstrapping first node")
    servers = [await manager.server_add() for _ in range(2)]

    logger.info(f"Decommissioning node {servers[0]}")
    await manager.decommission_node(servers[0].server_id)
    servers = servers[1:]

    logger.info(f"Restarting node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_start(servers[0].server_id)
