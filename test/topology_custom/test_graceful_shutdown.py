#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import pytest
import logging
import asyncio
import re

from test.pylib.internal_types import IPAddress, HostID, ServerNum
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_graceful_shutdown(manager: ManagerClient) -> None:
    logger.info(f"Booting initial cluster")
    servers = await manager.servers_add(5)

    patterns = []
    patterns += [
        r'Compaction for .* deliberately stopped',
        r'update compaction history failed:.*ignored',
    ]
    # ignore expected rpc errors when nodes are stopped.
    expected_rpc_errors = [
        'connection dropped: connection is closed',
        'connection dropped: .*Connection reset by peer',
        'connection dropped: Semaphore broken',
        'fail to connect: Connection refused',
        'fail to connect: Connection reset by peer',
        'server stream connection dropped: invalid type specifier',
        'server stream connection dropped: Unknown parent connection',
    ]
    # we may stop nodes that have not finished starting yet
    patterns += [r'(Startup|start) failed: seastar::sleep_aborted',
                 r'Timer callback failed: seastar::gate_closed_exception',
                 ]
    patterns += ["rpc - client .*({})".format('|'.join(expected_rpc_errors))]
    patterns += [" raft_rpc - Failed to send "]
    # We see benign rpc errors when nodes start/stop.
    # If they cause system malfunction, it should be detected using higher-level tests.
    patterns += [r'rpc::unknown_verb_error']
    pattern = re.compile('|'.join(["({})".format(p) for p in set(patterns)]))

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
            for line, _ in matches:
                if not pattern.search(line):
                    pytest.fail(f"unexpected ERROR detected in {srv_id}: {line}")

    tasks = [asyncio.create_task(task(s.server_id)) for s in servers]
    await asyncio.gather(*tasks)
