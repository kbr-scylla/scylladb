#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test replacing node in different scenarios
"""
import time
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
import pytest
import asyncio
import logging


@pytest.mark.asyncio
async def test_replace_different_ip(manager: ManagerClient) -> None:
    """Replace an existing node with new node using a different IP address"""
    servers = await manager.running_servers()
    await manager.server_stop(servers[0].server_id)
    logging.info(f"Replacing {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    srv = await manager.server_add(replace_cfg)

    logging.info("sleep")
    await asyncio.sleep(6)
    logging.info("restart")
    await manager.server_restart(servers[1].server_id)
    logging.info("sleep")
    await asyncio.sleep(18)
    logging.info(f"decommission {srv}")
    await manager.decommission_node(srv.server_id)
    logging.info("sleep")
    await asyncio.sleep(2)
    cql = manager.cql
    assert(cql)
    rs = list(await cql.run_async("select * from system.cluster_status"))
    print(rs)
    assert(len(rs) == 3)
