"""Tests for the @cached decorator — sync and async functions."""

from __future__ import annotations

from zerocache import ZeroCache, cached


class TestCachedDecorator:
    def test_sync_function_cached(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        def compute(n: int) -> int:
            nonlocal call_count
            call_count += 1
            return n * 2

        assert compute(5) == 10
        assert compute(5) == 10  # cache hit
        assert call_count == 1

    def test_sync_different_args_separate_entries(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        def compute(n: int) -> int:
            nonlocal call_count
            call_count += 1
            return n * 2

        compute(1)
        compute(2)
        assert call_count == 2

    def test_sync_custom_key_prefix(self, cache: ZeroCache):
        @cached(key_prefix="myprefix", ttl=60, cache=cache)
        def fn(x: int) -> int:
            return x

        fn(42)
        keys = cache.keys("myprefix:*")
        assert len(keys) == 1

    async def test_async_function_cached(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        async def fetch(user_id: int) -> str:
            nonlocal call_count
            call_count += 1
            return f"user_{user_id}"

        result1 = await fetch(1)
        result2 = await fetch(1)  # cache hit
        assert result1 == result2 == "user_1"
        assert call_count == 1

    async def test_async_different_args(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        async def fetch(user_id: int) -> str:
            nonlocal call_count
            call_count += 1
            return f"user_{user_id}"

        await fetch(1)
        await fetch(2)
        assert call_count == 2

    def test_cached_with_kwargs(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        def compute(a: int, b: int = 0) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        compute(1, b=2)
        compute(1, b=2)  # same kwargs → cache hit
        assert call_count == 1

    def test_cached_different_kwargs(self, cache: ZeroCache):
        call_count = 0

        @cached(ttl=60, cache=cache)
        def compute(a: int, b: int = 0) -> int:
            nonlocal call_count
            call_count += 1
            return a + b

        compute(1, b=2)
        compute(1, b=3)  # different kwargs → separate cache entry
        assert call_count == 2
