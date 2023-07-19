import asyncio
import logging
import pytest
import time
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_snapshot_too_large_mutation_repro(manager: ManagerClient) -> None:
    # await manager.server_add()

    cql = manager.cql
    await cql.run_async("CREATE KEYSPACE feeds WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND durable_writes = true;")
    async def task(i):
        now = time.time()
        logger.info(f"Creating table {i}")
        await cql.run_async(f"""
            CREATE TABLE feeds.table{i} (
                field1 text PRIMARY KEY,
                field2 text,
                field3 date,
                field4 text
            ) WITH bloom_filter_fp_chance = 0.01
                AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
                AND comment = ''
                AND compaction = {{'class': 'SizeTieredCompactionStrategy'}}
                AND compression = {{}}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';
        """)
        logger.info(f"Creating index {i}")
        await cql.run_async(f"""
            CREATE INDEX IF NOT EXISTS table{i}_field4_table{i} ON feeds.table{i} (field4);
        """)
        after = time.time()
        dur = after - now
        times.append(dur)
        logger.info(f"Created index {i}, time: {dur}")

    times = []
    for i in range(65):
        tasks = [asyncio.create_task(task(j)) for j in range(i*10, (i+1)*10)]
        await asyncio.gather(*tasks)

    logger.info(f"times: {times}")
    await manager.server_add()

    await cql.run_async("DROP KEYSPACE feeds;")
