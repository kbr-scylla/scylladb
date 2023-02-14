import pytest

@pytest.mark.asyncio
async def test_boot(manager, random_tables):
    await manager.mark_dirty()
