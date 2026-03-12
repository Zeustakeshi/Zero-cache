"""Tests for SortedSet data type: zadd/zscore/zrank/zcard/zrange/zrangebyscore/zrem/zincrby."""

from __future__ import annotations

from zerocache import ZeroCache


class TestZadd:
    def test_zadd_and_zscore(self, cache: ZeroCache):
        cache.zadd("zs", {"alice": 1.0, "bob": 2.0})
        assert cache.zscore("zs", "alice") == 1.0
        assert cache.zscore("zs", "bob") == 2.0

    def test_zadd_update_score(self, cache: ZeroCache):
        cache.zadd("zs", {"alice": 1.0})
        cache.zadd("zs", {"alice": 5.0})
        assert cache.zscore("zs", "alice") == 5.0

    def test_zadd_missing_key(self, cache: ZeroCache):
        assert cache.zscore("nope", "member") is None


class TestZrank:
    def test_zrank_ascending(self, cache: ZeroCache):
        cache.zadd("zs", {"low": 1.0, "mid": 5.0, "high": 10.0})
        assert cache.zrank("zs", "low") == 0
        assert cache.zrank("zs", "mid") == 1
        assert cache.zrank("zs", "high") == 2

    def test_zrank_missing_member(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0})
        assert cache.zrank("zs", "nope") is None


class TestZcard:
    def test_zcard(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zcard("zs") == 3

    def test_zcard_missing(self, cache: ZeroCache):
        assert cache.zcard("nope") == 0


class TestZrange:
    def test_zrange_all(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrange("zs", 0, -1) == ["a", "b", "c"]

    def test_zrange_slice(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0})
        assert cache.zrange("zs", 1, 2) == ["b", "c"]

    def test_zrange_with_scores(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0})
        result = cache.zrange("zs", 0, -1, with_scores=True)
        assert result == [(1.0, "a"), (2.0, "b")]

    def test_zrange_missing(self, cache: ZeroCache):
        assert cache.zrange("nope", 0, -1) == []


class TestZrangebyscore:
    def test_zrangebyscore(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 5.0, "c": 10.0})
        assert cache.zrangebyscore("zs", 1.0, 5.0) == ["a", "b"]

    def test_zrangebyscore_no_match(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0})
        assert cache.zrangebyscore("zs", 50.0, 100.0) == []


class TestZrem:
    def test_zrem_existing(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0})
        assert cache.zrem("zs", "a") == 1
        assert cache.zscore("zs", "a") is None

    def test_zrem_missing_member(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0})
        assert cache.zrem("zs", "nope") == 0

    def test_zrem_multiple(self, cache: ZeroCache):
        cache.zadd("zs", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert cache.zrem("zs", "a", "b", "nope") == 2


class TestZincrby:
    def test_zincrby_new_member(self, cache: ZeroCache):
        score = cache.zincrby("zs", "alice", 5.0)
        assert score == 5.0
        assert cache.zscore("zs", "alice") == 5.0

    def test_zincrby_existing(self, cache: ZeroCache):
        cache.zadd("zs", {"alice": 10.0})
        score = cache.zincrby("zs", "alice", 3.0)
        assert score == 13.0
