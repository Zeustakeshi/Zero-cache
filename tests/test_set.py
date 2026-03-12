"""Tests for Set data type: sadd/srem/smembers/sismember/scard/sinter/sunion/sdiff/spop."""

from __future__ import annotations

from zerocache import ZeroCache


class TestSaddSrem:
    def test_sadd_new_members(self, cache: ZeroCache):
        result = cache.sadd("s", "a", "b", "c")
        assert result == 3
        assert cache.smembers("s") == {"a", "b", "c"}

    def test_sadd_duplicate_not_counted(self, cache: ZeroCache):
        cache.sadd("s", "a", "b")
        result = cache.sadd("s", "b", "c")
        assert result == 1  # only "c" is new
        assert cache.smembers("s") == {"a", "b", "c"}

    def test_srem_existing(self, cache: ZeroCache):
        cache.sadd("s", "a", "b", "c")
        assert cache.srem("s", "b") == 1
        assert cache.smembers("s") == {"a", "c"}

    def test_srem_missing_member(self, cache: ZeroCache):
        cache.sadd("s", "a")
        assert cache.srem("s", "nope") == 0

    def test_srem_missing_key(self, cache: ZeroCache):
        assert cache.srem("nope", "a") == 0


class TestSismemberScard:
    def test_sismember_true(self, cache: ZeroCache):
        cache.sadd("s", "a")
        assert cache.sismember("s", "a") is True

    def test_sismember_false(self, cache: ZeroCache):
        cache.sadd("s", "a")
        assert cache.sismember("s", "b") is False

    def test_scard(self, cache: ZeroCache):
        cache.sadd("s", "a", "b", "c")
        assert cache.scard("s") == 3

    def test_scard_missing(self, cache: ZeroCache):
        assert cache.scard("nope") == 0


class TestSetOperations:
    def test_sinter(self, cache: ZeroCache):
        cache.sadd("s1", "a", "b", "c")
        cache.sadd("s2", "b", "c", "d")
        assert cache.sinter("s1", "s2") == {"b", "c"}

    def test_sunion(self, cache: ZeroCache):
        cache.sadd("s1", "a", "b")
        cache.sadd("s2", "b", "c")
        assert cache.sunion("s1", "s2") == {"a", "b", "c"}

    def test_sdiff(self, cache: ZeroCache):
        cache.sadd("s1", "a", "b", "c")
        cache.sadd("s2", "b", "c")
        assert cache.sdiff("s1", "s2") == {"a"}

    def test_sinter_empty_result(self, cache: ZeroCache):
        cache.sadd("s1", "a")
        cache.sadd("s2", "b")
        assert cache.sinter("s1", "s2") == set()


class TestSpop:
    def test_spop_returns_member(self, cache: ZeroCache):
        cache.sadd("s", "only")
        result = cache.spop("s")
        assert result == "only"
        assert cache.scard("s") == 0

    def test_spop_missing(self, cache: ZeroCache):
        assert cache.spop("nope") is None
