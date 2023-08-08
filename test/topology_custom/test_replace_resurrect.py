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
    cfg = {'experimental_features': list[str]()}
    servers = [await manager.server_add(config=cfg) for _ in range(3)]
    await manager.server_stop(servers[0].server_id)
    logging.info(f"Replacing {servers[0]}")
    replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False, use_host_id = False)
    srv = await manager.server_add(replace_cfg, config=cfg)

    logging.info("sleep")
    await asyncio.sleep(6)
    logging.info("restart")
    await manager.server_restart(servers[1].server_id)
    logging.info("sleep")
    await asyncio.sleep(18)
    cql = manager.cql
    assert(cql)
    rs = list(await cql.run_async("select * from system.cluster_status"))
    logging.info(f"cluster_status: {rs}")
    logging.info(f"decommission {srv}")
    await manager.decommission_node(srv.server_id)
    logging.info("sleep")
    await asyncio.sleep(15)
    rs = list(await cql.run_async("select * from system.cluster_status"))
    logging.info(f"cluster_status: {rs}")
    for r in rs:
        assert(r.peer != servers[0].ip_addr or int(r.tokens) == 0)
