import pytest
import logging
import asyncio
import random
import time
from contextlib import contextmanager

from cassandra.auth import PlainTextAuthProvider # type: ignore
from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts

@contextmanager
def conn(ip):
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([ip]),
                               request_timeout=200)
    with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                 contact_points=[ip],
                 protocol_version=4,
                 auth_provider=auth) as cluster:
        with cluster.connect() as session:
            yield session


async def test_data_loss(manager: ManagerClient, random_tables: RandomTables) -> None:
    for it in range(20):
        logging.info(f"Iteration {it}")

        for _ in range(3):
            s = await manager.server_add()
            logging.info(f"Added server {s}")
        servers = await manager.running_servers()
        logging.info(f"Running servers after adding: {servers}")

        cql = manager.cql
        ks = random_tables.keyspace
        assert(cql)
        await cql.run_async(f"create table {ks}.t (pk int, ck int, primary key (pk, ck))")

        stop = False
        written = set()
        async def write() -> None:
            nonlocal stop, written, cql
            assert(cql)

            stmt = cql.prepare(f"insert into {ks}.t (pk, ck) values (?, ?)")
            stmt.consistency_level = ConsistencyLevel.ALL
            i = 0
            while not stop:
                for j in range(20):
                    await cql.run_async(stmt, (i, j))
                    written.add((i, j))
                    logging.info(f"added ({i}, {j})")
                    await asyncio.sleep(0.001)
                i += 1

        write_task = asyncio.create_task(write())

        stmt = cql.prepare(f"select * from {ks}.t")
        stmt.consistency_level = ConsistencyLevel.ONE
        stmt.fetch_size = 1000000

        for s in servers[:-3]:
            logging.info(f"Decommissioning {s}")
            await manager.decommission_node(s.server_id)

            logging.info(f"Finished decommissioning {s}, obtaining copy of written")
            curr = written.copy()
            for srv in (await manager.running_servers()):
                host = (await wait_for_cql_and_get_hosts(cql, [srv], time.time() + 10))[0]
                logging.info(f"Reading from {host}")
                rs = await cql.run_async(stmt, host=host)
                logging.info(f"Obtained rows from {host}")
                read = set((r[0], r[1]) for r in rs)
                missing = curr - read
                if missing:
                    stop = True
                    await write_task

                    logging.error(f"Failed to observe rows on {host}: {missing}")
                    #logging.info(f"Read: {read}")

                    stmt = cql.prepare(f"select * from {ks}.t where pk = ? and ck = ?")
                    stmt.consistency_level = ConsistencyLevel.ONE
                    missing_twice = {(i, j) for i, j in missing if not list(await cql.run_async(stmt, (i, j), host=host))}
                    logging.info(f"Missing twice on {host}: {missing_twice}")

                    stmt.consistency_level = ConsistencyLevel.ALL
                    missing_all = {(i, j) for i, j in missing if not list(await cql.run_async(stmt, (i, j), host=host))}
                    logging.info(f"Missing CL=ALL on {host}: {missing_all}")

                    assert not missing, f"Failed to observe rows on {host}: {missing}\nmissing twice {missing_twice}\nmissing CL=ALL {missing_all}"

        if not stop:
            stop = True
            await write_task

        await cql.run_async(f"drop table {ks}.t")
