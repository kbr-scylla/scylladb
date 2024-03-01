#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import reconnect_driver, restart, enter_recovery_state, \
        delete_raft_data_and_upgrade_state, log_run_time, wait_until_upgrade_finishes as wait_until_schema_upgrade_finishes, \
        wait_until_topology_upgrade_finishes, delete_raft_topology_state, check_system_topology_and_cdc_generations_v3_consistency


@pytest.mark.asyncio
@log_run_time
async def test_topology_recovery_after_majority_loss(request, manager: ManagerClient):
    servers = await manager.servers_add(5)
    cql = manager.cql
    assert(cql)

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    srv1, srv2, *others = servers
    rec = [srv1, srv2]

    logging.info(f"Killing all nodes except {rec}")
    await asyncio.gather(*(manager.server_stop_gracefully(srv.server_id) for srv in others))

    for r in rec:
        logging.info(f"Entering recovery state on {r}")
        host1 = next(h for h in hosts if h.address == r.ip_addr)
        await enter_recovery_state(cql, host1)
        await restart(manager, r)

    await manager.server_sees_other_server(srv1.ip_addr, srv2.ip_addr)
    await manager.server_sees_other_server(srv2.ip_addr, srv1.ip_addr)

    cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    host1 = (await wait_for_cql_and_get_hosts(cql, [srv1], time.time() + 60))[0]
    host2 = (await wait_for_cql_and_get_hosts(cql, [srv2], time.time() + 60))[0]
    hs = [host1, host2]

    for i in range(len(others)):
        to_remove = others[i]
        ignore_dead_ips = [srv.ip_addr for srv in others[i+1:]]
        logging.info(f"Removing {to_remove} using {srv1} with ignore_dead: {ignore_dead_ips}")
        await manager.remove_node(srv1.server_id, to_remove.server_id, ignore_dead_ips)

    for r, h in zip(rec, hs):
        logging.info(f"Deleting old Raft data and upgrade state on {h} and restarting")
        await delete_raft_topology_state(cql, h)
        await delete_raft_data_and_upgrade_state(cql, h)
        await restart(manager, r)
        cql = await reconnect_driver(manager)

    logging.info("Node restarted, waiting until driver connects")
    await wait_for_cql_and_get_hosts(cql, rec, time.time() + 60)

    logging.info("Waiting until upgrade to raft schema finishes.")
    for h in hs:
        await wait_until_schema_upgrade_finishes(cql, h, time.time() + 60)

    logging.info("Triggering upgrade to raft topology")
    await manager.api.upgrade_to_raft_topology(host1.address)

    logging.info("Waiting until upgrade to raft topology finishes")
    for h in hs:
        await wait_until_topology_upgrade_finishes(manager, h.address, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hs)

    logging.info("Add two more nodes")
    servers = rec + await manager.servers_add(2)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Checking consistency of data in system.topology and system.cdc_generations_v3")
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
