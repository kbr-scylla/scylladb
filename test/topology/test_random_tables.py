#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: ScyllaDB-Proprietary
#
import pytest
from cassandra.protocol import InvalidRequest                            # type: ignore


# Simple test of schema helper
@pytest.mark.asyncio
async def test_new_table(manager, random_tables):
    cql = manager.cql
    assert cql is not None
    table = await random_tables.add_table(ncolumns=5)
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" \
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    pk_col = table.columns[0]
    ck_col = table.columns[1]
    vals = [pk_col.val(1), ck_col.val(1)]
    res = await cql.run_async(f"SELECT * FROM {table} WHERE {pk_col}=%s AND {ck_col}=%s",
                              parameters=vals)
    assert len(res) == 1
    assert list(res[0])[:2] == vals
    await random_tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    await random_tables.verify_schema()


# Simple test of schema helper with alter
@pytest.mark.asyncio
async def test_alter_verify_schema(manager, random_tables):
    """Verify table schema"""
    cql = manager.cql
    assert cql is not None
    await random_tables.add_tables(ntables=4, ncolumns=5)
    await random_tables.verify_schema()
    # Manually remove a column
    table = random_tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_new_table_insert_one(manager, random_tables):
    cql = manager.cql
    assert cql is not None
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()
    pk_col = table.columns[0]
    ck_col = table.columns[1]
    vals = [pk_col.val(1), ck_col.val(1)]
    res = await cql.run_async(f"SELECT * FROM {table} WHERE pk=%s AND {ck_col}=%s",
                              parameters=vals)
    assert len(res) == 1
    assert list(res[0])[:2] == vals


@pytest.mark.asyncio
async def test_drop_column(manager, random_tables):
    """Drop a random column from a table"""
    cql = manager.cql
    assert cql is not None
    table = await random_tables.add_table(ncolumns=5)
    await table.insert_seq()
    await table.drop_column()
    res = (await cql.run_async(f"SELECT * FROM {table} WHERE pk=%s",
                               parameters=[table.columns[0].val(1)]))[0]
    assert len(res) == 4
    await table.drop_column()
    res = (await cql.run_async(f"SELECT * FROM {table} WHERE pk=%s",
                               parameters=[table.columns[0].val(1)]))[0]
    assert len(res) == 3
    await random_tables.verify_schema(table)


@pytest.mark.asyncio
async def test_add_index(random_tables):
    """Add and drop an index"""
    table = await random_tables.add_table(ncolumns=5)
    with pytest.raises(AssertionError, match='partition key'):
        await table.add_index(0)
    await table.add_index(2)
    await random_tables.verify_schema(table)
