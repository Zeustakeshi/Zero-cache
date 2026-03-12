"""
zerocache._pipeline
~~~~~~~~~~~~~~~~~~~
Batch-command executor (Pipeline) for ZeroCache.

Queues commands locally and executes them in one shot,
eliminating per-command overhead.

Example::

    results = (
        cache.pipeline()
            .set("a", 1)
            .set("b", 2)
            .hset("user:1", "name", "Alice")
            .execute()
    )
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from zerocache._core import ZeroCache

__all__ = ["Pipeline"]


class Pipeline:
    """Batch-command executor.

    Collects commands via chained method calls and runs them all
    together on :meth:`execute` / :meth:`async_execute`.

    Args:
        cache: The :class:`ZeroCache` instance to execute commands on.
    """

    def __init__(self, cache: ZeroCache) -> None:
        self._c = cache
        self._cmds: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _q(self, method: str, *args: Any, **kwargs: Any) -> Pipeline:
        self._cmds.append((method, args, kwargs))
        return self

    # ── command builders ───────────────────────────────────────────────

    def set(self, k: str, v: Any, ttl: int = 0, **kw: Any) -> Pipeline:
        return self._q("set", k, v, ttl=ttl, **kw)

    def get(self, k: str) -> Pipeline:
        return self._q("get", k)

    def delete(self, *k: str) -> Pipeline:
        return self._q("delete", *k)

    def incr(self, k: str, n: int = 1) -> Pipeline:
        return self._q("incr", k, n)

    def hset(self, k: str, f: str, v: Any) -> Pipeline:
        return self._q("hset", k, f, v)

    def hget(self, k: str, f: str) -> Pipeline:
        return self._q("hget", k, f)

    def lpush(self, k: str, *v: Any) -> Pipeline:
        return self._q("lpush", k, *v)

    def rpush(self, k: str, *v: Any) -> Pipeline:
        return self._q("rpush", k, *v)

    def sadd(self, k: str, *v: Any) -> Pipeline:
        return self._q("sadd", k, *v)

    def zadd(self, k: str, m: dict[str, float]) -> Pipeline:
        return self._q("zadd", k, m)

    def expire(self, k: str, t: int) -> Pipeline:
        return self._q("expire", k, t)

    def mset(self, mapping: dict[str, Any], ttl: int = 0) -> Pipeline:
        return self._q("mset", mapping, ttl=ttl)

    # ── execution ──────────────────────────────────────────────────────

    def execute(self) -> list[Any]:
        """Run all queued commands and return their results as a list."""
        results = [getattr(self._c, m)(*a, **kw) for m, a, kw in self._cmds]
        self._cmds.clear()
        return results

    async def async_execute(self) -> list[Any]:
        """Async variant — offloads execution to thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.execute)

    def __repr__(self) -> str:
        return f"Pipeline(queued={len(self._cmds)})"
