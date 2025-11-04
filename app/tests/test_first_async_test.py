import pytest
import asyncio

@pytest.mark.asyncio
async def test_first_async_test():
    
    async def task1():
        await asyncio.sleep(0.5)
        return True

    results = await asyncio.gather(*[task1() for _ in range(10)])

    assert all(results)