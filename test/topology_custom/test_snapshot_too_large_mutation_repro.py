import asyncio
import logging
import pytest
import time
from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_snapshot_too_large_mutation_repro(manager: ManagerClient) -> None:
    await manager.server_add()

    cql = manager.get_cql()
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
        logger.info(f"Created index {i}, time: {dur}")

    # The test from issue #13864 used 5k tables with segment_size = 32mb
    # scaling it down ~4x to speed up the test: 1300 tables, segment_size = 8mb

    batch_size = 100
    for i in range(1300 // batch_size):
        tasks = [asyncio.create_task(task(j)) for j in range(i*batch_size, (i+1)*batch_size)]
        await asyncio.gather(*tasks)

    await manager.server_add(config={
        'schema_commitlog_segment_size_in_mb': 8,
    })

    await cql.run_async("DROP KEYSPACE feeds;")
