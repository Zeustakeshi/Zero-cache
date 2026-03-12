"""
Async usage example for ZeroCache.

Demonstrates async_get/async_set, @cached decorator, and pub/sub
all running inside an asyncio event loop.
"""

import asyncio

from zerocache import ZeroCache, cached


async def basic_async_ops():
    cache = ZeroCache(load_on_start=False)

    await cache.async_set("user:1", {"name": "Alice", "role": "admin"}, ttl=60)
    user = await cache.async_get("user:1")
    print(f"User: {user}")

    await cache.async_hset("session:abc", "token", "xyz123")
    token = await cache.async_hget("session:abc", "token")
    print(f"Token: {token}")

    await cache.async_lpush("queue", "job1", "job2", "job3")
    items = await cache.async_lrange("queue", 0, -1)
    print(f"Queue: {items}")

    cache.shutdown()


async def cached_decorator_example():
    cache = ZeroCache(load_on_start=False)
    call_count = 0

    @cached(ttl=30, cache=cache)
    async def fetch_user(user_id: int):
        nonlocal call_count
        call_count += 1
        # Simulate network call
        await asyncio.sleep(0.01)
        return {"id": user_id, "name": f"User {user_id}"}

    user = await fetch_user(42)
    print(f"Fetched: {user} (call #{call_count})")

    cached_user = await fetch_user(42)
    print(f"Cached:  {cached_user} (call #{call_count})")  # still 1

    cache.shutdown()


async def pubsub_example():
    cache = ZeroCache(load_on_start=False)

    results = []

    async def listener():
        queue = await cache.subscribe("events")
        for _ in range(3):
            msg = await asyncio.wait_for(queue.get(), timeout=2.0)
            results.append(msg)
        cache.unsubscribe("events", queue)

    async def publisher():
        await asyncio.sleep(0.05)
        for i in range(3):
            await cache.publish("events", {"event": f"tick_{i}"})
            await asyncio.sleep(0.01)

    await asyncio.gather(listener(), publisher())
    print(f"Received {len(results)} events: {results}")
    cache.shutdown()


if __name__ == "__main__":
    print("=== Basic Async Ops ===")
    asyncio.run(basic_async_ops())

    print("\n=== Cached Decorator ===")
    asyncio.run(cached_decorator_example())

    print("\n=== Pub/Sub ===")
    asyncio.run(pubsub_example())
