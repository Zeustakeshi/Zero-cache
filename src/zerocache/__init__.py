"""
ZeroCache — Redis-like in-memory cache engine for Python.

Zero dependencies. O(log n) TTL. 16-shard locking. Async-native.

Example::

    from zerocache import ZeroCache, cached

    cache = ZeroCache()
    cache.set("hello", "world", ttl=60)
    cache.get("hello")  # → "world"

    @cached(ttl=300)
    async def get_user(user_id: int): ...
"""

from zerocache._core      import ZeroCache, get_cache
from zerocache._decorators import cached
from zerocache._sorted_set import SortedSet
from zerocache._types      import DataType

__version__ = "1.1.1"
__author__  = "ZeroCache Contributors"
__all__ = [
    "ZeroCache",
    "get_cache",
    "cached",
    "SortedSet",
    "DataType",
]
