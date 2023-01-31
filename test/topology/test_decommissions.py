import pytest
import logging
import asyncio
import time

from typing import Optional

from cassandra.cluster import NoHostAvailable, Session # type: ignore
from cassandra.pool import Host # type: ignore

from test.pylib.util import wait_for
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables

logger = logging.getLogger(__name__)

async def get_token_ring_host_ids(manager: ManagerClient, srv: ServerInfo) -> set[str]:
    """Get the host IDs of normal token owners known by `srv`."""
    token_endpoint_map = await manager.api.client.get_json("/storage_service/tokens_endpoint", srv.ip_addr)
    normal_endpoints = {e["value"] for e in token_endpoint_map}
    logger.info(f"Normal endpoints' IPs by {srv}: {normal_endpoints}")
    host_id_map = await manager.api.client.get_json('/storage_service/host_id', srv.ip_addr)
    all_host_ids = {e["value"] for e in host_id_map}
    logger.info(f"All host IDs by {srv}: {all_host_ids}")
    normal_host_ids = {e["value"] for e in host_id_map if e["key"] in normal_endpoints}
    logger.info(f"Normal endpoints' host IDs by {srv}: {normal_host_ids}")
    return normal_host_ids

async def get_current_group0_config(manager: ManagerClient, srv: ServerInfo) -> set[tuple[str, bool]]:
    """Get the current Raft group 0 configuration known by `srv`.
       The first element of each tuple is the Raft ID of the node (which is equal to the Host ID),
       the second element indicates whether the node is a voter.
     """
    assert(manager.cql)
    host = (await wait_for_cql_and_get_hosts(manager.cql, [srv], time.time() + 60))[0]
    group0_id = (await manager.cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'",
        host=host))[0].value
    config = await manager.cql.run_async(
        f"select server_id, can_vote from system.raft_state where group_id = {group0_id} and disposition = 'CURRENT'",
        host=host)
    result = {(str(m.server_id), bool(m.can_vote)) for m in config}
    logger.info(f"Group 0 members by {srv}: {result}")
    return result

async def wait_for_cql(cql: Session, host: Host, deadline: float) -> None:
    async def cql_ready():
        try:
            await cql.run_async("select * from system.local", host=host)
        except NoHostAvailable:
            logger.info(f"Driver not connected to {host} yet")
            return None
        return True
    await wait_for(cql_ready, deadline)


async def wait_for_cql_and_get_hosts(cql: Session, servers: list[ServerInfo], deadline: float) \
        -> list[Host]:
    """Wait until every server in `servers` is available through `cql`
       and translate `servers` to a list of Cassandra `Host`s.
    """
    ip_set = set(str(srv.ip_addr) for srv in servers)
    async def get_hosts() -> Optional[list[Host]]:
        hosts = cql.cluster.metadata.all_hosts()
        remaining = ip_set - {h.address for h in hosts}
        if not remaining:
            return hosts

        logger.info(f"Driver hasn't yet learned about hosts: {remaining}")
        return None
    hosts = await wait_for(get_hosts, deadline)

    # Take only hosts from `ip_set` (there may be more)
    hosts = [h for h in hosts if h.address in ip_set]
    await asyncio.gather(*(wait_for_cql(cql, h, deadline) for h in hosts))

    return hosts

async def wait_for_token_ring_and_group0_consistency(manager: ManagerClient, deadline: float) -> None:
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        async def token_ring_matches():
            token_ring_ids = await get_token_ring_host_ids(manager, srv)
            diff = token_ring_ids ^ group0_ids
            if diff:
                logger.warning(f"Group 0 members and token ring members don't yet match" \
                               f" according to {srv}, symmetric difference: {diff}")
                return None
            return True
        await wait_for(token_ring_matches, deadline, period=.5)

@pytest.mark.asyncio
async def test_decommissions(manager: ManagerClient, random_tables) -> None:
    for _ in range(10):
        s = await manager.server_add()
        logger.info(f"Added server {s}")
    servers = await manager.running_servers()
    logger.info(f"Running servers after adding: {servers}")
    for s in servers[:-1]:
        logger.info(f"Decommissioning {s}")
        await manager.decommission_node(s.server_id)
        await wait_for_token_ring_and_group0_consistency(manager, time.time() + 10)

