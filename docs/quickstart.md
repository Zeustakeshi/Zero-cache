# Quickstart

## Installation

```bash
pip install zero-cache
```

## Basic Usage

```python
from zerocache import ZeroCache

cache = ZeroCache()

# Strings
cache.set("greeting", "hello", ttl=60)
print(cache.get("greeting"))   # → "hello"

# Hash
cache.hset("user:1", "name", "Alice")
print(cache.hget("user:1", "name"))   # → "Alice"

# List
cache.rpush("tasks", "buy milk", "write code")
print(cache.lpop("tasks"))   # → "buy milk"

# Set
cache.sadd("languages", "python", "go", "rust")
print("python" in cache.smembers("languages"))   # → True

# Sorted Set (leaderboard)
cache.zadd("scores", {"alice": 100, "bob": 80, "carol": 95})
print(cache.zrange("scores", 0, -1))   # → ["bob", "carol", "alice"]
```

## Using with Async

```python
import asyncio
from zerocache import ZeroCache

cache = ZeroCache()

async def main():
    await cache.async_set("key", "value", ttl=30)
    val = await cache.async_get("key")
    print(val)   # → "value"

asyncio.run(main())
```

## The `@cached` Decorator

```python
from zerocache import cached

@cached(ttl=300)
async def get_user(user_id: int):
    # Only called once per unique user_id per 300 seconds
    return await db.fetch_user(user_id)
```

## Shutdown Gracefully

```python
cache.shutdown()   # saves snapshot + joins background thread
```
