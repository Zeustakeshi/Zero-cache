"""
zerocache._decorators
~~~~~~~~~~~~~~~~~~~~~
Function-level caching decorator.

Example::

    from zerocache import cached

    @cached(key_prefix="user", ttl=60)
    async def get_user(user_id: int): ...

    @cached(ttl=300)
    def compute_heavy(date: str): ...
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from zerocache._core import ZeroCache

__all__ = ["cached"]


def cached(
    key_prefix: str = "",
    ttl: int = 300,
    cache: ZeroCache | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Cache decorator for sync and async functions.

    The cache key is built from *key_prefix* (or the function's
    ``__qualname__``) plus a BLAKE2s hash of the positional and keyword
    arguments, so different inputs produce different cache entries.

    Args:
        key_prefix: Prefix for the cache key.  Defaults to the
                    function's ``__qualname__``.
        ttl:        Time-to-live in seconds (default 300).
        cache:      Explicit :class:`~zerocache.ZeroCache` instance.
                    Falls back to the module-level default instance.

    Returns:
        A decorator that transparently wraps sync or async functions.

    Example::

        @cached(key_prefix="user", ttl=60)
        async def get_user(user_id: int):
            return await db.fetch(user_id)

        @cached(ttl=300)
        def compute_heavy(date: str):
            ...
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:

        @wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            from zerocache._core import _default

            _c = _default if cache is None else cache
            h = hashlib.blake2s(
                (str(args) + str(sorted(kwargs.items()))).encode(),
                digest_size=8,
            ).hexdigest()
            key = f"{key_prefix or fn.__qualname__}:{h}"
            result = _c.get(key)
            if result is None:
                result = await fn(*args, **kwargs)
                _c.set(key, result, ttl=ttl)
            return result

        @wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            from zerocache._core import _default

            _c = _default if cache is None else cache
            h = hashlib.blake2s(
                (str(args) + str(sorted(kwargs.items()))).encode(),
                digest_size=8,
            ).hexdigest()
            key = f"{key_prefix or fn.__qualname__}:{h}"
            result = _c.get(key)
            if result is None:
                result = fn(*args, **kwargs)
                _c.set(key, result, ttl=ttl)
            return result

        return async_wrapper if asyncio.iscoroutinefunction(fn) else sync_wrapper

    return decorator
