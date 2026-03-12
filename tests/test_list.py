"""Tests for List data type: lpush/rpush/lpop/rpop/lrange/llen/lindex/lset."""

from __future__ import annotations

from zerocache import ZeroCache


class TestLpushRpush:
    def test_rpush_appends(self, cache: ZeroCache):
        cache.rpush("list", "a", "b", "c")
        assert cache.lrange("list", 0, -1) == ["a", "b", "c"]

    def test_lpush_prepends(self, cache: ZeroCache):
        cache.lpush("list", "a", "b", "c")
        # lpush("list", "a","b","c") → "a" ends at front (first arg prepended last)
        # ZeroCache: reversed(values) means c→b→a inserted at 0 → ["a","b","c"]
        assert cache.lrange("list", 0, -1) == ["a", "b", "c"]

    def test_rpush_returns_length(self, cache: ZeroCache):
        assert cache.rpush("list", 1) == 1
        assert cache.rpush("list", 2) == 2

    def test_lpush_returns_length(self, cache: ZeroCache):
        assert cache.lpush("list", "x") == 1
        assert cache.lpush("list", "y") == 2


class TestLpopRpop:
    def test_lpop_single(self, cache: ZeroCache):
        cache.rpush("list", "a", "b", "c")
        assert cache.lpop("list") == "a"
        assert cache.lrange("list", 0, -1) == ["b", "c"]

    def test_rpop_single(self, cache: ZeroCache):
        cache.rpush("list", "a", "b", "c")
        assert cache.rpop("list") == "c"
        assert cache.lrange("list", 0, -1) == ["a", "b"]

    def test_lpop_count(self, cache: ZeroCache):
        cache.rpush("list", 1, 2, 3, 4)
        result = cache.lpop("list", 2)
        assert result == [1, 2]
        assert cache.llen("list") == 2

    def test_rpop_count(self, cache: ZeroCache):
        cache.rpush("list", 1, 2, 3, 4)
        result = cache.rpop("list", 2)
        assert result == [3, 4]
        assert cache.llen("list") == 2

    def test_pop_empty_returns_none(self, cache: ZeroCache):
        assert cache.lpop("nope") is None
        assert cache.rpop("nope") is None


class TestLrange:
    def test_lrange_all(self, cache: ZeroCache):
        cache.rpush("list", 1, 2, 3)
        assert cache.lrange("list", 0, -1) == [1, 2, 3]

    def test_lrange_slice(self, cache: ZeroCache):
        cache.rpush("list", 1, 2, 3, 4, 5)
        assert cache.lrange("list", 1, 3) == [2, 3, 4]

    def test_lrange_missing_key(self, cache: ZeroCache):
        assert cache.lrange("nope", 0, -1) == []


class TestLlenLindexLset:
    def test_llen(self, cache: ZeroCache):
        cache.rpush("list", "a", "b", "c")
        assert cache.llen("list") == 3

    def test_llen_missing(self, cache: ZeroCache):
        assert cache.llen("nope") == 0

    def test_lindex(self, cache: ZeroCache):
        cache.rpush("list", "x", "y", "z")
        assert cache.lindex("list", 0) == "x"
        assert cache.lindex("list", 2) == "z"
        assert cache.lindex("list", -1) == "z"

    def test_lindex_out_of_bounds(self, cache: ZeroCache):
        cache.rpush("list", "a")
        assert cache.lindex("list", 99) is None

    def test_lset(self, cache: ZeroCache):
        cache.rpush("list", "a", "b", "c")
        assert cache.lset("list", 1, "B") is True
        assert cache.lindex("list", 1) == "B"

    def test_lset_out_of_bounds(self, cache: ZeroCache):
        cache.rpush("list", "a")
        assert cache.lset("list", 99, "x") is False
