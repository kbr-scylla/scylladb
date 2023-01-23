#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
import time
import asyncio
import logging

from typing import Callable, Awaitable, Optional, TypeVar, Generic

from cassandra.cluster import NoHostAvailable, Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host                         # type: ignore # pylint: disable=no-name-in-module

from test.pylib.internal_types import ServerInfo

class LogPrefixAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['prefix'], msg), kwargs


unique_name_prefix = 'test_'
T = TypeVar('T')


def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)


async def wait_for(
        pred: Callable[[], Awaitable[Optional[T]]],
        deadline: float, period: float = 1) -> T:
    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        res = await pred()
        if res is not None:
            return res
        await asyncio.sleep(period)


async def wait_for_cql(cql: Session, host: Host, deadline: float) -> None:
    async def cql_ready():
        try:
            await cql.run_async("select * from system.local", host=host)
        except NoHostAvailable:
            logging.info(f"Driver not connected to {host} yet")
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

        logging.info(f"Driver hasn't yet learned about hosts: {remaining}")
        return None
    hosts = await wait_for(get_hosts, deadline)

    # Take only hosts from `ip_set` (there may be more)
    hosts = [h for h in hosts if h.address in ip_set]
    await asyncio.gather(*(wait_for_cql(cql, h, deadline) for h in hosts))

    return hosts


unique_name.last_ms = 0
