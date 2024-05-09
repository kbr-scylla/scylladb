import asyncio
import pytest
import time
import logging

from cassandra.cluster import ConnectionException, ConsistencyLevel, NoHostAvailable, Session, SimpleStatement  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_mv_decommission(manager: ManagerClient):
    cfg = {'error_injections_at_startup': ['force_gossip_based_join']}

    servers = [await manager.server_add(config=cfg, timeout=60) for _ in range(4)]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3};")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    await cql.run_async("CREATE materialized view ks.t_view AS select pk, v from ks.t where v is not null primary key (v, pk)")

    stop_event = asyncio.Event()
    concurrency = 10
    async def do_writes() -> int:
        iteration = 0
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(f"insert into ks.t (pk, v) values ({iteration}, {iteration})")
            except NoHostAvailable as e:
                for _, err in e.errors.items():
                    # ConnectionException can be raised when the node is shutting down.
                    if not isinstance(err, ConnectionException):
                        logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                        raise
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                raise
            iteration += 1
            await asyncio.sleep(0.01)

        return iteration

    tasks = [asyncio.create_task(do_writes()) for _ in range(concurrency)]

    async def finish() -> list[int]:
        logger.info("Stopping write workers")
        stop_event.set()
        nums = await asyncio.gather(*tasks)
        return nums

    await manager.decommission_node(servers[-1].server_id)

    nums = await finish()

    await asyncio.sleep(2)

    logger.info(nums)
    logger.info(sum(nums))

    res = await cql.run_async("select pk, v from ks.t");
    for r in res:
        logger.info(f"pk: {r.pk}, v: {r.v}")

    res = await cql.run_async("select v, pk from ks.t_view");
    for r in res:
        logger.info(f"v: {r.v}, pk: {r.pk}")
