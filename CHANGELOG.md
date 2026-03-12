# Changelog

All notable changes to ZeroCache will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.1] - 2024-03-12

### Fixed
- **BUG-1**: `LRUStore.copy()` — `OrderedDict.copy()` calls `self.__class__()`
  which triggers `LRUStore.__init__(maxsize=...)` without arguments → `TypeError`.
  Fixed by overriding `copy()` to return a plain `dict`.

---

## [1.1.0] - 2024-03-01

### Added
- `DataType` `IntEnum` replacing plain string dtype field
- `sys.intern()` for key strings — repeated keys share one object
- `version` field on `CacheEntry` for TTL heap invalidation

### Changed
- `CacheEntry` drops `created_at` and `accessed_at` fields (-16 bytes/key)
- Background worker simplified — single sweep loop

### Performance
- Integer dtype comparison vs string → ~3x faster dtype checks
- Interned keys → faster dict hash lookups

---

## [1.0.0] - 2024-01-01

### Added
- Initial release
- `ZeroCache` with 16-shard LRU store
- String / Hash / List / Set / SortedSet data types
- Heap-based TTL expiry (100 ms resolution)
- Crash-safe persistence with zlib compression
- Async interface (`async_get`, `async_set`, etc.)
- Pipeline batch execution
- Pub/Sub with asyncio queues
- `@cached` decorator for sync and async functions
- `get_cache()` FastAPI dependency
