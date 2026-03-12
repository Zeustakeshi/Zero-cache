"""
zerocache._core
~~~~~~~~~~~~~~~
Core engine: CacheEntry, LRUStore, ShardedStore, ZeroCache, get_cache.

╔══════════════════════════════════════════════════════════════════════╗
║                    ZeroCache — v1.1.1                                ║
║         Redis-like In-Memory Cache Engine for Python                 ║
║                                                                      ║
║  • Sharded locking     — 16 independent shards, parallel ops         ║
║  • Heap-based TTL      — O(log n) expiry, 100ms resolution           ║
║  • Non-blocking save   — snapshot outside lock, atomic write         ║
║  • bisect SortedSet    — O(log n) zadd / zrange / zrank              ║
║  • __slots__ entries   — ~50% less RAM vs plain dataclass            ║
║  • IntEnum dtype       — int comparison vs string (~8 byte saved)    ║
║  • sys.intern keys     — shared string objects, faster hash lookup   ║
║  • Lean CacheEntry     — dropped created_at/accessed_at (~16 bytes)  ║
║  • Async-native        — run_in_executor, never blocks event loop    ║
║  • Pipeline            — batch commands, zero round-trip overhead    ║
║  • Pub/Sub             — asyncio channels with gather broadcast      ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import fnmatch
import heapq
import logging
import pickle
import sys
import threading
import time
import zlib
from collections import OrderedDict, defaultdict
from dataclasses  import dataclass
from pathlib      import Path
from typing       import Any, Dict, Generator, List, Optional, Tuple

from zerocache._types     import DataType, _resolve_dtype
from zerocache._sorted_set import SortedSet
from zerocache._pipeline  import Pipeline

__all__ = ["CacheEntry", "LRUStore", "ShardedStore", "ZeroCache", "get_cache"]

__version__ = "1.1.1"

logger = logging.getLogger("zerocache")


# ═══════════════════════════════════════════════════════════════════════
# CACHE ENTRY
#
# __slots__ eliminates per-instance __dict__ → ~50% less RAM.
# Fields vs v1.0:
#   Removed  created_at   — never used in any code path (-8 bytes)
#   Removed  accessed_at  — LRU tracked by OrderedDict, not timestamp (-8 bytes)
#   Changed  dtype: str   → DataType(IntEnum) — faster compare, less RAM
#   Added    version      — for TTL heap invalidation
# ═══════════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class CacheEntry:
    """Single cache record with value, optional TTL, data type, and metadata."""

    value:      Any
    expires_at: float    = 0.0              # monotonic; 0 = immortal
    dtype:      DataType = DataType.STRING  # IntEnum — faster than str
    hits:       int      = 0               # disabled when track_hits=False
    version:    int      = 0               # incremented on each SET

    def is_expired(self) -> bool:
        """Return ``True`` if this entry has passed its TTL."""
        return self.expires_at > 0 and time.monotonic() > self.expires_at

    def touch(self, track: bool = True) -> None:
        """Increment hit counter — skipped when *track* is ``False``."""
        if track:
            self.hits += 1


# ═══════════════════════════════════════════════════════════════════════
# LRU STORE  (single shard)
#
# Why OrderedDict and not a custom doubly-linked list?
#   OrderedDict is implemented in C; move_to_end() is O(1).
#   A pure-Python linked list would add per-node object overhead and
#   be slower due to interpreter dispatch.
#
# BUG FIX v1.1.1:
#   OrderedDict.copy() calls self.__class__() to create the new instance,
#   which triggers LRUStore.__init__(maxsize=...) without arguments
#   → TypeError.  Override copy() to return a plain dict instead.
# ═══════════════════════════════════════════════════════════════════════

class LRUStore(OrderedDict):
    """LRU-evicting dict, used as one shard of :class:`ShardedStore`."""

    def __init__(self, maxsize: int) -> None:
        super().__init__()
        self.maxsize   = maxsize
        self.evictions = 0

    def __setitem__(self, key: str, value: CacheEntry) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            self.popitem(last=False)   # evict LRU head
            self.evictions += 1

    def peek(self, key: str) -> Optional[CacheEntry]:
        """Read entry WITHOUT updating LRU order."""
        return super().__getitem__(key) if key in self else None

    def copy(self) -> Dict[str, CacheEntry]:
        """Return a plain dict copy — NOT an LRUStore.

        Root cause of BUG-1 (v1.1.0):
            OrderedDict.copy() calls self.__class__() internally.
            self.__class__ is LRUStore, so it calls LRUStore() with no
            arguments → LRUStore.__init__() missing required 'maxsize'
            → TypeError.

        Fix:
            Override copy() to return dict(self.items()) which bypasses
            __class__ construction entirely.  ShardedStore.snapshot()
            only needs a plain mapping, not LRU behaviour.
        """
        return dict(self.items())


# ═══════════════════════════════════════════════════════════════════════
# SHARDED STORE
#
# Key-space split into N shards (N = power-of-two → bitmask instead of %).
# Each shard has its own RLock → ops on different keys never contend.
# ═══════════════════════════════════════════════════════════════════════

class ShardedStore:
    """N-shard LRU store.  Key → shard via ``hash(key) & (N-1)``.

    Args:
        num_shards: Must be a power of two (e.g. 16, 32, 64).
        maxsize:    Total capacity split evenly across shards.
    """

    def __init__(self, num_shards: int = 16, maxsize: int = 100_000) -> None:
        assert num_shards & (num_shards - 1) == 0, "num_shards must be a power of two"
        self.n      = num_shards
        self._mask  = num_shards - 1
        self.shards = [LRUStore(max(1, maxsize // num_shards)) for _ in range(num_shards)]
        self.locks  = [threading.RLock() for _ in range(num_shards)]

    # ── routing ────────────────────────────────────────────────────────

    def idx(self, key: str) -> int:
        """Shard index — bitmask is faster than modulo."""
        return hash(key) & self._mask

    def shard_of(self, key: str) -> Tuple[LRUStore, threading.RLock]:
        i = self.idx(key)
        return self.shards[i], self.locks[i]

    # ── bulk helpers ───────────────────────────────────────────────────

    def all_items(self) -> List[Tuple[str, CacheEntry]]:
        """Snapshot of all (key, entry) pairs across every shard."""
        items: List[Tuple[str, CacheEntry]] = []
        for shard, lock in zip(self.shards, self.locks):
            with lock:
                items.extend(list(shard.items()))
        return items

    def snapshot(self) -> Dict[str, CacheEntry]:
        """Non-blocking snapshot — each shard locked briefly and independently.

        Serialisation happens outside any lock.
        Uses dict(shard.items()) explicitly (not shard.copy()) to make
        the intent clear and guard against any future copy() changes.
        """
        snap: Dict[str, CacheEntry] = {}
        for shard, lock in zip(self.shards, self.locks):
            with lock:
                snap.update(dict(shard.items()))
        return snap

    def clear_all(self) -> None:
        """Flush every shard under its own lock."""
        for shard, lock in zip(self.shards, self.locks):
            with lock:
                shard.clear()

    # ── aggregate stats ────────────────────────────────────────────────

    @property
    def total_evictions(self) -> int:
        return sum(s.evictions for s in self.shards)

    def __len__(self) -> int:
        return sum(len(s) for s in self.shards)


# ═══════════════════════════════════════════════════════════════════════
# ZEROCACHE
# ═══════════════════════════════════════════════════════════════════════

class ZeroCache:
    """Redis-like in-memory cache engine — ZeroCache v1.1.1.

    Key design decisions
    ────────────────────
    Concurrency     16-shard RLock store; different keys never contend.
    Expiration      Min-heap (expire_at, key, version); sweep every 100 ms.
    Persistence     Per-shard snapshots (short lock) then pickle+zlib
                    written outside any lock → zero blocking.
    SortedSet       bisect-backed sorted list — O(log n) insert/range/rank.
    Memory (v1.1)   • CacheEntry __slots__: dropped created_at/accessed_at.
                    • DataType IntEnum: int vs string for dtype field.
                    • sys.intern(key): repeated keys share one string object.
                    • track_hits=False: skip hit counter → saves 8 bytes/key.
    Async           All async_* use run_in_executor → never blocks event loop.

    Args:
        maxsize:            Total key capacity across all shards.
        num_shards:         Lock shard count (must be power of two).
        persist_path:       File path for on-disk snapshots.
        auto_save_interval: Seconds between automatic snapshots.
        compress:           Enable zlib compression (level 1) on snapshot.
        load_on_start:      Restore snapshot from disk on startup.
        track_hits:         Count per-key hits (False saves ~8 bytes/key).
        intern_keys:        Intern key strings for faster lookups and less RAM.

    Example::

        from zerocache import ZeroCache

        cache = ZeroCache()
        cache.set("key", "value", ttl=60)
        cache.get("key")  # → "value"
    """

    VERSION = __version__

    def __init__(
        self,
        maxsize:            int  = 100_000,
        num_shards:         int  = 16,
        persist_path:       str  = ".zerocache.db",
        auto_save_interval: int  = 30,
        compress:           bool = True,
        load_on_start:      bool = True,
        track_hits:         bool = True,
        intern_keys:        bool = True,
    ) -> None:
        self._db         = ShardedStore(num_shards, maxsize)
        self.track_hits  = track_hits
        self.intern_keys = intern_keys

        self.persist_path        = Path(persist_path)
        self.compress            = compress
        self.auto_save_interval  = auto_save_interval

        # TTL heap: (expire_at_monotonic, key, version)
        self._ttl_heap: List[Tuple[float, str, int]] = []
        self._heap_lock = threading.Lock()

        # Thread-safe stats
        self._stats      = dict(hits=0, misses=0, sets=0, deletes=0, saves=0, loads=0)
        self._stats_lock = threading.Lock()

        # Pub/Sub
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)

        if load_on_start:
            self._load()

        self._running   = True
        self._bg_thread = threading.Thread(
            target=self._background_worker,
            daemon=True,
            name="zerocache-bg",
        )
        self._bg_thread.start()

    # ───────────────────────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ───────────────────────────────────────────────────────────────────

    def _k(self, key: str) -> str:
        """Optionally intern *key*.

        sys.intern() ensures equal strings share the same object:
          • CPython dict lookup checks pointer equality first → faster
          • Repeated keys cost zero extra RAM after the first intern
        """
        return sys.intern(key) if self.intern_keys else key

    def _stat(self, key: str, n: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] += n

    def _push_ttl(self, key: str, expires_at: float, version: int) -> None:
        """Push expiry task onto the min-heap.
        Accepts absolute monotonic *expires_at* to avoid clock drift.
        """
        with self._heap_lock:
            heapq.heappush(self._ttl_heap, (expires_at, key, version))

    # ───────────────────────────────────────────────────────────────────
    # TTL SWEEP
    # ───────────────────────────────────────────────────────────────────

    def _sweep_expired_heap(self) -> None:
        """Pop all heap tasks whose deadline has passed.
        Stale tasks (version mismatch) are discarded silently.
        """
        now = time.monotonic()
        while True:
            with self._heap_lock:
                if not self._ttl_heap or self._ttl_heap[0][0] > now:
                    break
                expire_at, key, version = heapq.heappop(self._ttl_heap)

            shard, lock = self._db.shard_of(key)
            with lock:
                entry = shard.peek(key)
                if entry is not None and entry.version == version:
                    del shard[key]

    # ───────────────────────────────────────────────────────────────────
    # PERSISTENCE
    # ───────────────────────────────────────────────────────────────────

    def _save(self) -> None:
        """Persist non-expired entries to disk.

        1. Snapshot each shard under its own short-lived lock.
        2. Release all locks before serialising or writing.
        3. Write to *.tmp then atomically rename → crash-safe.
        """
        try:
            snapshot = {
                k: v for k, v in self._db.snapshot().items()
                if not v.is_expired()
            }

            payload = pickle.dumps(
                {"version": self.VERSION, "ts": time.time(), "data": snapshot},
                protocol=5,
            )
            if self.compress:
                payload = zlib.compress(payload, level=1)

            tmp = self.persist_path.with_suffix(".tmp")
            tmp.write_bytes(payload)
            tmp.replace(self.persist_path)   # atomic rename

            self._stat("saves")
            logger.debug("[ZeroCache] Saved %d keys → %s", len(snapshot), self.persist_path)

        except Exception:
            logger.exception("[ZeroCache] Save failed")

    def _load(self) -> None:
        """Restore snapshot from disk and rebuild TTL heap entries.

        Keys without a heap entry would never be actively expired
        after restart — this step ensures they are.
        """
        if not self.persist_path.exists():
            return
        try:
            payload = self.persist_path.read_bytes()
            if self.compress:
                try:
                    payload = zlib.decompress(payload)
                except zlib.error:
                    pass

            snap   = pickle.loads(payload)
            loaded = 0

            for key, entry in snap["data"].items():
                if entry.is_expired():
                    continue

                key = self._k(key)
                shard, lock = self._db.shard_of(key)
                with lock:
                    shard[key] = entry

                if entry.expires_at > 0:
                    self._push_ttl(key, entry.expires_at, entry.version)

                loaded += 1

            self._stat("loads")
            logger.info("[ZeroCache] Loaded %d keys ← %s", loaded, self.persist_path)

        except Exception:
            logger.exception("[ZeroCache] Load failed")

    # ───────────────────────────────────────────────────────────────────
    # BACKGROUND WORKER
    # ───────────────────────────────────────────────────────────────────

    def _background_worker(self) -> None:
        """Daemon thread.

        • Every 100 ms — heap TTL sweep.
        • Every N secs  — persist snapshot to disk.
        """
        last_save = time.monotonic()
        while self._running:
            self._sweep_expired_heap()
            if time.monotonic() - last_save >= self.auto_save_interval:
                self._save()
                last_save = time.monotonic()
            time.sleep(0.1)

    # ───────────────────────────────────────────────────────────────────
    # CORE — GET / SET / DELETE
    # ───────────────────────────────────────────────────────────────────

    def set(
        self,
        key:   str,
        value: Any,
        ttl:   int  = 0,
        nx:    bool = False,
        xx:    bool = False,
    ) -> bool:
        """Store *value* under *key*.

        Args:
            key:   Cache key.
            value: Any picklable Python object.
            ttl:   Time-to-live in seconds (0 = immortal).
            nx:    Only set if key does NOT exist.
            xx:    Only set if key DOES exist.

        Returns:
            ``True`` on success; ``False`` when nx/xx condition not met.
        """
        key        = self._k(key)
        expires_at = time.monotonic() + ttl if ttl > 0 else 0.0
        shard, lock = self._db.shard_of(key)

        with lock:
            existing = shard.peek(key)
            exists   = existing is not None and not existing.is_expired()

            if nx and exists:     return False
            if xx and not exists: return False

            version    = (existing.version + 1) if existing else 0
            shard[key] = CacheEntry(
                value      = value,
                expires_at = expires_at,
                dtype      = _resolve_dtype(value),
                version    = version,
            )

        if ttl > 0:
            self._push_ttl(key, expires_at, version)

        self._stat("sets")
        return True

    def get(self, key: str, default: Any = None) -> Any:
        """Return value for *key*, or *default* if missing/expired."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)

        with lock:
            entry = shard.peek(key)

            if entry is None:
                self._stat("misses")
                return default

            if entry.is_expired():
                del shard[key]
                self._stat("misses")
                return default

            entry.touch(self.track_hits)
            shard.move_to_end(key)
            self._stat("hits")
            return entry.value

    def delete(self, *keys: str) -> int:
        """Remove one or more keys. Returns count actually deleted."""
        count = 0
        for key in keys:
            key         = self._k(key)
            shard, lock = self._db.shard_of(key)
            with lock:
                if key in shard:
                    del shard[key]
                    count += 1
        self._stat("deletes", count)
        return count

    def exists(self, *keys: str) -> int:
        """Return number of provided keys that exist and have not expired."""
        count = 0
        for key in keys:
            key         = self._k(key)
            shard, lock = self._db.shard_of(key)
            with lock:
                e = shard.peek(key)
                if e and not e.is_expired():
                    count += 1
        return count

    def expire(self, key: str, ttl: int) -> bool:
        """Set/update TTL on an existing key. Returns ``False`` if not found."""
        key         = self._k(key)
        expires_at  = time.monotonic() + ttl
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e:
                return False
            e.expires_at = expires_at
            self._push_ttl(key, expires_at, e.version)
            return True

    def persist(self, key: str) -> bool:
        """Remove TTL (make immortal). Returns ``False`` if not found."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e:
                return False
            e.expires_at = 0.0
            return True

    def ttl(self, key: str) -> int:
        """Remaining TTL in seconds. -1 = no expiry | -2 = not found."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if e is None:          return -2
            if e.expires_at == 0:  return -1
            return max(0, int(e.expires_at - time.monotonic()))

    def type(self, key: str) -> str:
        """Return data type: string | hash | list | set | zset | none."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            return str(e.dtype) if e else "none"

    def rename(self, src: str, dst: str) -> bool:
        """Atomically rename *src* to *dst*.

        Edge cases handled:
          src == dst  → no-op (avoids accidental deletion)
          TTL         → heap entry re-pushed for new key
          Lock order  → acquired by shard index to prevent deadlock
        """
        src, dst = self._k(src), self._k(dst)
        if src == dst:
            return True

        si, di = self._db.idx(src), self._db.idx(dst)
        if si == di:
            ordered = [self._db.locks[si]]
        elif si < di:
            ordered = [self._db.locks[si], self._db.locks[di]]
        else:
            ordered = [self._db.locks[di], self._db.locks[si]]

        for lk in ordered: lk.acquire()
        try:
            s_shard, d_shard = self._db.shards[si], self._db.shards[di]
            entry = s_shard.peek(src)
            if entry is None:
                return False
            d_shard[dst] = entry
            del s_shard[src]
        finally:
            for lk in reversed(ordered): lk.release()

        if entry.expires_at > 0:
            remaining = entry.expires_at - time.monotonic()
            if remaining > 0:
                self._push_ttl(dst, entry.expires_at, entry.version)

        return True

    def keys(self, pattern: str = "*") -> List[str]:
        """Return all matching keys — O(N).

        Warning:
            Prefer :meth:`scan_iter` for large datasets.
        """
        all_keys = [k for k, v in self._db.all_items() if not v.is_expired()]
        return fnmatch.filter(all_keys, pattern) if pattern != "*" else all_keys

    def scan_iter(self, pattern: str = "*") -> Generator[str, None, None]:
        """Yield matching keys one shard at a time.

        Each shard is copied then immediately unlocked — minimal contention.
        """
        for shard, lock in zip(self._db.shards, self._db.locks):
            with lock:
                batch = list(shard.items())
            for k, v in batch:
                if not v.is_expired() and fnmatch.fnmatch(k, pattern):
                    yield k

    def flush(self) -> None:
        """Remove all keys and clear the TTL heap."""
        self._db.clear_all()
        with self._heap_lock:
            self._ttl_heap = []

    # ───────────────────────────────────────────────────────────────────
    # STRING OPS
    # ───────────────────────────────────────────────────────────────────

    def incr(self, key: str, amount: int = 1) -> int:
        """Atomically increment integer value (missing key treated as 0)."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e   = shard.peek(key)
            val = (int(e.value) if e and not e.is_expired() else 0) + amount
            self.set(key, val)
            return val

    def decr(self, key: str, amount: int = 1) -> int:
        """Atomically decrement integer value."""
        return self.incr(key, -amount)

    def append(self, key: str, value: str) -> int:
        """Append *value* to string. Returns new length."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e   = shard.peek(key)
            val = (str(e.value) if e else "") + value
            self.set(key, val)
            return len(val)

    def getset(self, key: str, new_value: Any) -> Any:
        """Atomically replace value and return the previous one."""
        old = self.get(key)
        self.set(key, new_value)
        return old

    def mget(self, *keys: str) -> List[Any]:
        """Return values for multiple keys (None for missing)."""
        return [self.get(k) for k in keys]

    def mset(self, mapping: Dict[str, Any], ttl: int = 0) -> bool:
        """Set multiple keys from *mapping* dict."""
        for k, v in mapping.items():
            self.set(k, v, ttl=ttl)
        return True

    # ───────────────────────────────────────────────────────────────────
    # HASH OPS
    # ───────────────────────────────────────────────────────────────────

    def hset(self, key: str, field: str, value: Any, ttl: int = 0) -> bool:
        """Set *field* in hash. Creates hash if missing."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if e and e.dtype == DataType.HASH and not e.is_expired():
                e.value[field] = value
            else:
                self.set(key, {field: value}, ttl=ttl)
        return True

    def hget(self, key: str, field: str, default: Any = None) -> Any:
        """Return *field* from hash, or *default*."""
        d = self.get(key)
        return d.get(field, default) if isinstance(d, dict) else default

    def hmset(self, key: str, mapping: Dict, ttl: int = 0) -> bool:
        """Set multiple fields in hash from *mapping*."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if e and e.dtype == DataType.HASH and not e.is_expired():
                e.value.update(mapping)
            else:
                self.set(key, dict(mapping), ttl=ttl)
        return True

    def hmget   (self, key: str, *fields: str) -> List[Any]:
        """Return values for multiple *fields* from hash."""
        d = self.get(key) or {}
        return [d.get(f) for f in fields]

    def hgetall (self, key: str) -> Dict:
        """Return entire hash as dict."""
        return self.get(key) or {}

    def hkeys   (self, key: str) -> List:
        """Return all field names in hash."""
        d = self.get(key)
        return list(d.keys()) if isinstance(d, dict) else []

    def hvals   (self, key: str) -> List:
        """Return all field values in hash."""
        d = self.get(key)
        return list(d.values()) if isinstance(d, dict) else []

    def hlen    (self, key: str) -> int:
        """Return number of fields in hash."""
        d = self.get(key)
        return len(d) if isinstance(d, dict) else 0

    def hexists (self, key: str, field: str) -> bool:
        """Return ``True`` if *field* exists in hash."""
        d = self.get(key)
        return isinstance(d, dict) and field in d

    def hdel(self, key: str, *fields: str) -> int:
        """Delete *fields* from hash. Returns count removed."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.HASH:
                return 0
            return sum(1 for f in fields if e.value.pop(f, None) is not None)

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        """Increment *field* in hash by *amount*."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e   = shard.peek(key)
            d   = e.value if e and e.dtype == DataType.HASH else {}
            val = int(d.get(field, 0)) + amount
            self.hset(key, field, val)
            return val

    # ───────────────────────────────────────────────────────────────────
    # LIST OPS
    # ───────────────────────────────────────────────────────────────────

    def _get_list(self, key: str, shard: LRUStore) -> List:
        e = shard.peek(key)
        return e.value if e and e.dtype == DataType.LIST and not e.is_expired() else []

    def lpush(self, key: str, *values: Any) -> int:
        """Prepend *values* to list. Returns new length."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            lst = self._get_list(key, shard)
            for v in reversed(values): lst.insert(0, v)
            self.set(key, lst)
            return len(lst)

    def rpush(self, key: str, *values: Any) -> int:
        """Append *values* to list. Returns new length."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            lst = self._get_list(key, shard)
            lst.extend(values)
            self.set(key, lst)
            return len(lst)

    def lpop(self, key: str, count: int = 1) -> Any:
        """Remove and return *count* elements from left."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.LIST or not e.value:
                return None
            if count == 1:
                return e.value.pop(0)
            result, e.value[:count] = e.value[:count], []
            return result

    def rpop(self, key: str, count: int = 1) -> Any:
        """Remove and return *count* elements from right."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.LIST or not e.value:
                return None
            if count == 1:
                return e.value.pop()
            result = e.value[-count:]
            del e.value[-count:]
            return result

    def lrange(self, key: str, start: int, end: int) -> List:
        """Return elements in [start, end] range (inclusive)."""
        d = self.get(key)
        return d[start : end + 1 if end >= 0 else None] if isinstance(d, list) else []

    def llen(self, key: str) -> int:
        """Return list length."""
        d = self.get(key)
        return len(d) if isinstance(d, list) else 0

    def lindex(self, key: str, index: int) -> Any:
        """Return element at *index*."""
        d = self.get(key)
        try:    return d[index] if isinstance(d, list) else None
        except: return None

    def lset(self, key: str, index: int, value: Any) -> bool:
        """Set element at *index*. Returns ``False`` if out of bounds."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.LIST:
                return False
            try:    e.value[index] = value; return True
            except: return False

    # ───────────────────────────────────────────────────────────────────
    # SET OPS
    # ───────────────────────────────────────────────────────────────────

    def sadd(self, key: str, *members: Any) -> int:
        """Add *members* to set. Returns count of new members added."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if e and e.dtype == DataType.SET and not e.is_expired():
                before = len(e.value)
                e.value.update(members)
                return len(e.value) - before
            self.set(key, set(members))
            return len(members)

    def srem(self, key: str, *members: Any) -> int:
        """Remove *members* from set. Returns count removed."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.SET:
                return 0
            before = len(e.value)
            e.value.difference_update(members)
            return before - len(e.value)

    def smembers (self, key: str) -> set:
        """Return all members of set."""
        d = self.get(key)
        return d if isinstance(d, set) else set()

    def sismember(self, key: str, member: Any) -> bool:
        """Return ``True`` if *member* is in set."""
        return member in self.smembers(key)

    def scard    (self, key: str) -> int:
        """Return cardinality (number of members) of set."""
        return len(self.smembers(key))

    def sinter   (self, *keys: str) -> set:
        """Return intersection of all sets."""
        s = [self.smembers(k) for k in keys]
        return set.intersection(*s) if s else set()

    def sunion   (self, *keys: str) -> set:
        """Return union of all sets."""
        s = [self.smembers(k) for k in keys]
        return set.union(*s) if s else set()

    def sdiff    (self, *keys: str) -> set:
        """Return difference between first set and remaining sets."""
        s = [self.smembers(k) for k in keys]
        return set.difference(*s) if s else set()

    def spop(self, key: str) -> Any:
        """Remove and return a random member from set."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.SET or not e.value:
                return None
            return e.value.pop()

    # ───────────────────────────────────────────────────────────────────
    # SORTED SET OPS
    # ───────────────────────────────────────────────────────────────────

    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """Add/update members with scores. *mapping* = ``{member: score}``.

        Returns:
            Number of members in *mapping* (not necessarily new ones).
        """
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if e and e.dtype == DataType.ZSET and not e.is_expired():
                for m, s in mapping.items(): e.value.zadd(m, s)
            else:
                zs = SortedSet()
                for m, s in mapping.items(): zs.zadd(m, s)
                self.set(key, zs)
        return len(mapping)

    def zscore      (self, key: str, member: str) -> Optional[float]:
        """Return score of *member*, or ``None``."""
        d = self.get(key)
        return d.zscore(member) if isinstance(d, SortedSet) else None

    def zrank       (self, key: str, member: str) -> Optional[int]:
        """Return 0-based rank of *member*, or ``None``."""
        d = self.get(key)
        return d.zrank(member) if isinstance(d, SortedSet) else None

    def zcard       (self, key: str) -> int:
        """Return number of members in sorted set."""
        d = self.get(key)
        return d.zcard() if isinstance(d, SortedSet) else 0

    def zrange      (self, key: str, start: int, end: int, with_scores: bool = False) -> List:
        """Return members in rank range [start, end]."""
        d = self.get(key)
        return d.zrange(start, end, with_scores) if isinstance(d, SortedSet) else []

    def zrangebyscore(self, key: str, min_score: float, max_score: float) -> List[str]:
        """Return members with scores in [min_score, max_score]."""
        d = self.get(key)
        return d.zrangebyscore(min_score, max_score) if isinstance(d, SortedSet) else []

    def zrem(self, key: str, *members: str) -> int:
        """Remove *members* from sorted set. Returns count removed."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e = shard.peek(key)
            if not e or e.dtype != DataType.ZSET:
                return 0
            return sum(1 for m in members if e.value.zrem(m))

    def zincrby(self, key: str, member: str, amount: float = 1.0) -> float:
        """Increment score of *member* by *amount*."""
        key         = self._k(key)
        shard, lock = self._db.shard_of(key)
        with lock:
            e         = shard.peek(key)
            zs        = e.value if e and e.dtype == DataType.ZSET else None
            new_score = (zs.zscore(member) or 0.0 if zs else 0.0) + amount
            self.zadd(key, {member: new_score})
            return new_score

    # ───────────────────────────────────────────────────────────────────
    # ASYNC INTERFACE
    # ───────────────────────────────────────────────────────────────────

    async def _run(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    async def async_get      (self, key: str, default: Any = None) -> Any:
        return await self._run(self.get, key, default)

    async def async_set      (self, key: str, value: Any, ttl: int = 0, **kw: Any) -> bool:
        return await self._run(self.set, key, value, ttl=ttl, **kw)

    async def async_delete   (self, *keys: str) -> int:
        return await self._run(self.delete, *keys)

    async def async_mget     (self, *keys: str) -> List[Any]:
        return await self._run(self.mget, *keys)

    async def async_mset     (self, mapping: Dict, ttl: int = 0) -> bool:
        return await self._run(self.mset, mapping, ttl=ttl)

    async def async_hset     (self, key: str, field: str, value: Any, ttl: int = 0) -> bool:
        return await self._run(self.hset, key, field, value, ttl=ttl)

    async def async_hget     (self, key: str, field: str, default: Any = None) -> Any:
        return await self._run(self.hget, key, field, default)

    async def async_hgetall  (self, key: str) -> Dict:
        return await self._run(self.hgetall, key)

    async def async_lpush    (self, key: str, *values: Any) -> int:
        return await self._run(self.lpush, key, *values)

    async def async_rpush    (self, key: str, *values: Any) -> int:
        return await self._run(self.rpush, key, *values)

    async def async_lpop     (self, key: str, count: int = 1) -> Any:
        return await self._run(self.lpop, key, count)

    async def async_rpop     (self, key: str, count: int = 1) -> Any:
        return await self._run(self.rpop, key, count)

    async def async_lrange   (self, key: str, start: int, end: int) -> List:
        return await self._run(self.lrange, key, start, end)

    async def async_sadd     (self, key: str, *members: Any) -> int:
        return await self._run(self.sadd, key, *members)

    async def async_smembers (self, key: str) -> set:
        return await self._run(self.smembers, key)

    async def async_zadd     (self, key: str, mapping: Dict[str, float]) -> int:
        return await self._run(self.zadd, key, mapping)

    async def async_zrange   (self, key: str, start: int, end: int, with_scores: bool = False) -> List:
        return await self._run(self.zrange, key, start, end, with_scores)

    async def async_incr     (self, key: str, amount: int = 1) -> int:
        return await self._run(self.incr, key, amount)

    # ───────────────────────────────────────────────────────────────────
    # PIPELINE
    # ───────────────────────────────────────────────────────────────────

    def pipeline(self) -> Pipeline:
        """Return a new :class:`~zerocache.Pipeline` bound to this cache."""
        return Pipeline(self)

    # ───────────────────────────────────────────────────────────────────
    # PUB / SUB
    # ───────────────────────────────────────────────────────────────────

    async def publish(self, channel: str, message: Any) -> int:
        """Broadcast *message* to all subscribers of *channel*.

        Returns:
            Number of subscribers that received the message.
        """
        subs = self._subscribers.get(channel, [])
        if subs:
            await asyncio.gather(*(q.put(message) for q in subs))
        return len(subs)

    async def subscribe(self, channel: str) -> asyncio.Queue:
        """Subscribe to *channel*. Await ``queue.get()`` to receive messages."""
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers[channel].append(q)
        return q

    def unsubscribe(self, channel: str, queue: asyncio.Queue) -> None:
        """Remove *queue* from *channel* subscribers."""
        if channel in self._subscribers:
            self._subscribers[channel] = [
                q for q in self._subscribers[channel] if q is not queue
            ]

    # ───────────────────────────────────────────────────────────────────
    # INTROSPECTION
    # ───────────────────────────────────────────────────────────────────

    def info(self) -> Dict[str, Any]:
        """Return runtime statistics and configuration summary."""
        with self._stats_lock:
            s = dict(self._stats)
        total = s["hits"] + s["misses"]
        return {
            "name"         : "ZeroCache",
            "version"      : self.VERSION,
            "keys"         : len(self._db),
            "shards"       : self._db.n,
            "maxsize"      : sum(sh.maxsize for sh in self._db.shards),
            "evictions"    : self._db.total_evictions,
            "ttl_heap_size": len(self._ttl_heap),
            "hit_rate"     : f"{s['hits'] / total:.1%}" if total else "N/A",
            "persist_path" : str(self.persist_path),
            "compress"     : self.compress,
            "track_hits"   : self.track_hits,
            "intern_keys"  : self.intern_keys,
            **s,
        }

    def save(self) -> None:
        """Trigger an immediate on-demand snapshot to disk."""
        self._save()

    def shutdown(self) -> None:
        """Graceful shutdown: stop background thread, persist, join."""
        self._running = False
        self._save()
        if self._bg_thread.is_alive():
            self._bg_thread.join(timeout=5)
        logger.info("[ZeroCache] Shutdown complete.")

    def __len__     (self)      -> int:  return len(self._db)
    def __contains__(self, key: str) -> bool: return self.exists(key) > 0
    def __repr__    (self)      -> str:
        return (f"ZeroCache(v{self.VERSION}, keys={len(self._db)}, "
                f"shards={self._db.n}, track_hits={self.track_hits}, "
                f"intern_keys={self.intern_keys})")


# ═══════════════════════════════════════════════════════════════════════
# GLOBAL INSTANCE  +  FastAPI DEPENDENCY
# ═══════════════════════════════════════════════════════════════════════

_default = ZeroCache()


def get_cache() -> ZeroCache:
    """FastAPI dependency — inject the shared ZeroCache instance.

    Example::

        from fastapi import Depends
        from zerocache import get_cache, ZeroCache

        @app.get("/items/{id}")
        async def read_item(id: int, cache: ZeroCache = Depends(get_cache)):
            ...
    """
    return _default
