# ZeroCache 🚀

**Redis-like in-memory cache engine for Python — zero dependencies.**

[![CI](https://github.com/Zeustakeshi/zerocache/actions/workflows/ci.yml/badge.svg)](https://github.com/Zeustakeshi/zerocache/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/zero-cache)](https://pypi.org/project/zero-cache/)
[![Python](https://img.shields.io/pypi/pyversions/zero-cache)](https://pypi.org/project/zero-cache/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Coverage](https://codecov.io/gh/Zeustakeshi/zerocache/branch/main/graph/badge.svg)](https://codecov.io/gh/Zeustakeshi/zerocache)

---

## ✨ Features

| Feature | Details |
|---------|---------|
| **Zero dependencies** | Pure Python standard library only |
| **Redis-like API** | String/Hash/List/Set/SortedSet + Pipeline + Pub/Sub |
| **16-shard locking** | Per-shard RLock — parallel ops on different keys |
| **Heap-based TTL** | O(log n) expiry sweep every 100 ms |
| **Async-native** | `async_get/set/...` via `run_in_executor`, never blocks event loop |
| **Persistence** | Crash-safe atomic snapshot with zlib compression |
| **LRU eviction** | Per-shard LRU, O(1) eviction via OrderedDict |
| **`@cached` decorator** | Works with both sync and async functions |
| **FastAPI ready** | `get_cache()` as a Depends dependency |
| **Type annotations** | PEP 561 typed package, mypy strict compatible |

---

## 📦 Installation

```bash
pip install zero-cache
```

Requires Python 3.10+.

---

## ⚡ Quick Start

```python
from zerocache import ZeroCache

cache = ZeroCache()

# String
cache.set("user:name", "Alice", ttl=300)
cache.get("user:name")          # → "Alice"

# Hash
cache.hset("user:1", "name", "Alice")
cache.hget("user:1", "name")    # → "Alice"

# List
cache.rpush("queue", "task1", "task2")
cache.lpop("queue")             # → "task1"

# Set
cache.sadd("tags", "python", "cache")
cache.sismember("tags", "python")  # → True

# Sorted Set
cache.zadd("leaderboard", {"alice": 100, "bob": 85})
cache.zrange("leaderboard", 0, -1)  # → ["bob", "alice"]
```

---

## 🚀 Async Usage

```python
import asyncio
from zerocache import ZeroCache

cache = ZeroCache()

async def main():
    await cache.async_set("key", "value", ttl=60)
    result = await cache.async_get("key")
    print(result)  # → "value"

asyncio.run(main())
```

---

## 🎯 `@cached` Decorator

```python
from zerocache import cached

@cached(ttl=300)
async def get_user(user_id: int):
    return await db.fetch(user_id)

@cached(key_prefix="compute", ttl=60)
def compute_heavy(n: int) -> int:
    return n ** n
```

---

## 🔄 Pipeline

```python
results = (
    cache.pipeline()
        .set("a", 1)
        .set("b", 2)
        .incr("a")
        .get("b")
        .execute()
)
# → [True, True, 2, 2]
```

---

## 📡 Pub/Sub

```python
import asyncio
from zerocache import ZeroCache

cache = ZeroCache()

async def listener():
    queue = await cache.subscribe("events")
    msg = await queue.get()
    print(f"Received: {msg}")

async def publisher():
    await asyncio.sleep(0.1)
    await cache.publish("events", {"type": "user_login", "user_id": 42})

asyncio.run(asyncio.gather(listener(), publisher()))
```

---

## 🌐 FastAPI Integration

```python
from fastapi import FastAPI, Depends
from zerocache import ZeroCache, get_cache

app = FastAPI()

@app.get("/items/{item_id}")
async def read_item(item_id: int, cache: ZeroCache = Depends(get_cache)):
    cached = cache.get(f"item:{item_id}")
    if cached:
        return cached
    result = {"id": item_id, "name": "Widget"}
    cache.set(f"item:{item_id}", result, ttl=60)
    return result
```

---

## ⚙️ Configuration

```python
cache = ZeroCache(
    maxsize=100_000,          # Total key capacity
    num_shards=16,            # Lock shard count (power of 2)
    persist_path=".cache.db", # Snapshot file
    auto_save_interval=30,    # Seconds between auto-saves
    compress=True,            # zlib compression for snapshots
    load_on_start=True,       # Resume from disk on startup
    track_hits=True,          # Count per-key hits
    intern_keys=True,         # sys.intern() for less RAM
)
```

---

## 📊 Benchmark

Approximate throughput on a modern CPU (single thread):

| Operation | ops/sec |
|-----------|---------|
| `set` | ~1,200,000 |
| `get` (hit) | ~1,500,000 |
| `hset` | ~900,000 |
| `zadd` | ~700,000 |

---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

MIT — see [LICENSE](LICENSE).
