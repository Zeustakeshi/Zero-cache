"""Tests for Pipeline: batch command execution sync and async."""

from __future__ import annotations

from zerocache import ZeroCache


class TestPipelineSync:
    def test_pipeline_set_get(self, cache: ZeroCache):
        results = cache.pipeline().set("a", 1).set("b", 2).get("a").get("b").execute()
        assert results == [True, True, 1, 2]

    def test_pipeline_chained(self, cache: ZeroCache):
        results = (
            cache.pipeline()
            .set("counter", 0)
            .incr("counter")
            .incr("counter")
            .get("counter")
            .execute()
        )
        assert results[-1] == 2

    def test_pipeline_hset(self, cache: ZeroCache):
        cache.pipeline().hset("user", "name", "Alice").execute()
        assert cache.hget("user", "name") == "Alice"

    def test_pipeline_mset(self, cache: ZeroCache):
        cache.pipeline().mset({"x": 10, "y": 20}).execute()
        assert cache.get("x") == 10
        assert cache.get("y") == 20

    def test_pipeline_clears_after_execute(self, cache: ZeroCache):
        pipe = cache.pipeline().set("k", "v")
        pipe.execute()
        # Execute again — should be empty, all return None/0
        results = pipe.execute()
        assert results == []

    def test_pipeline_list_ops(self, cache: ZeroCache):
        results = cache.pipeline().rpush("lst", "a").rpush("lst", "b").rpush("lst", "c").execute()
        assert results == [1, 2, 3]

    def test_pipeline_delete(self, cache: ZeroCache):
        cache.set("del_me", "v")
        results = cache.pipeline().delete("del_me").execute()
        assert results == [1]
        assert cache.get("del_me") is None


class TestPipelineAsync:
    async def test_async_execute(self, cache: ZeroCache):
        results = await cache.pipeline().set("async_a", 100).get("async_a").async_execute()
        assert results == [True, 100]
