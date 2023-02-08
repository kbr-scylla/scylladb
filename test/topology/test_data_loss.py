import pytest
import logging
import asyncio
import random
import time

from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import ConsistencyLevel # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables

async def test_data_loss(manager: ManagerClient, random_tables: RandomTables) -> None:
    for it in range(10):
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
            logging.info(f"Reading from db")
            rs = await cql.run_async(stmt)
            logging.info(f"Obtained rows from db")
            read = set((r[0], r[1]) for r in rs)
            missing = curr - read
            if missing:
                stop = True
                await write_task

                logging.error(f"Failed to observe rows: {missing}")
                #logging.info(f"Read: {read}")

                stmt = cql.prepare(f"select * from {ks}.t where pk = ? and ck = ?")
                stmt.consistency_level = ConsistencyLevel.ONE
                missing_twice = {(i, j) for i, j in missing if not list(await cql.run_async(stmt, (i, j)))}
                logging.info(f"Missing twice: {missing_twice}")

                stmt.consistency_level = ConsistencyLevel.ALL
                missing_all = {(i, j) for i, j in missing if not list(await cql.run_async(stmt, (i, j)))}
                logging.info(f"Missing CL=ALL: {missing_all}")

                assert not missing, f"Failed to observe rows: {missing}\nmissing twice {missing_twice}\nmissing CL=ALL {missing_all}"

        if not stop:
            stop = True
            await write_task

        await cql.run_async(f"drop table {ks}.t")
