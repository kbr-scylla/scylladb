#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import pytest
import logging
import asyncio

from test.pylib.internal_types import IPAddress, HostID, ServerNum
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_graceful_shutdown(manager: ManagerClient) -> None:
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(5)

    async def task(srv_id: ServerNum) -> None:
        for _ in range(10):
            log = await manager.server_open_log(srv_id)
            mark = await log.mark()
            logger.info(f"Stopping {srv_id}")
            await manager.server_stop_gracefully(srv_id)
            logger.info(f"Starting {srv_id}")
            await manager.server_start(srv_id)
            logger.info(f"Started {srv_id}")
            matches = await log.grep("ERROR ", from_mark = mark)
            if matches:
                line, _ = matches[0]
                pytest.fail(f"ERROR detected in {srv_id}: {line}")

    tasks = [asyncio.create_task(task(s.server_id)) for s in servers]
    await asyncio.gather(*tasks)
