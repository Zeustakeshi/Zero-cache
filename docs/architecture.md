# Architecture

## Overview

ZeroCache is organized into four layers:

```
User Code
    │
    ▼
ZeroCache (public API)
    │
    ├── ShardedStore (16 shards, each LRUStore + RLock)
    ├── TTL Heap (min-heap, background sweep every 100ms)
    ├── Persistence Worker (save/load via pickle + zlib)
    └── Pub/Sub (asyncio.Queue per channel)
```

## Sharded Locking

The key space is split into 16 shards (configurable, must be power-of-2):

```
shard_index = hash(key) & (num_shards - 1)
```

Each shard has its own `threading.RLock`. Operations on different keys
never contend with each other, enabling high parallelism.

## LRU Eviction

Each shard is an `LRUStore(OrderedDict)`. When a shard exceeds its
`maxsize`, the least-recently-used entry is evicted via `popitem(last=False)`.
`OrderedDict` is implemented in C — `move_to_end()` is O(1).

## TTL (Time-To-Live)

Expiry uses a min-heap `[(expire_at, key, version), ...]`.

- On `set(key, value, ttl=N)`, a heap entry is pushed.
- Background thread sweeps the heap every 100 ms.
- Stale heap entries (version mismatch) are silently discarded.
- Lazy check on `get()` also removes expired entries.

This gives O(log n) pushes and efficient batch cleanup.

## Persistence

1. Each shard is snapshot independently under its own short lock.
2. All locks are released before serialization (pickle + optional zlib).
3. Written to `*.tmp` then atomically renamed → crash-safe.

## Memory Layout (v1.1, Python 3.11, 64-bit)

| Component | Bytes |
|-----------|-------|
| `CacheEntry` (5 fields, `__slots__`) | ~40 |
| Interned key string | ~49 |
| `LRUStore` dict entry | ~48 |
| **Total per key** | **~137** |

## `DataType` IntEnum

`dtype` is stored as an `IntEnum` instead of a plain string:
- Int comparison is faster than string comparison
- Small integers are cached by CPython (no heap allocation)

## `sys.intern()` for Keys

When `intern_keys=True`, key strings are interned:
- CPython dict lookup checks pointer equality first → faster
- Repeated keys share one object → less RAM
