"""Tests for Hash data type: hset/hget/hmset/hmget/hgetall/hkeys/hvals/hlen/hexists/hdel/hincrby."""

from __future__ import annotations

from zerocache import ZeroCache


class TestHset:
    def test_hset_and_hget(self, cache: ZeroCache):
        cache.hset("user:1", "name", "Alice")
        assert cache.hget("user:1", "name") == "Alice"

    def test_hget_missing_field(self, cache: ZeroCache):
        cache.hset("user:1", "name", "Alice")
        assert cache.hget("user:1", "age") is None
        assert cache.hget("user:1", "age", 0) == 0

    def test_hget_missing_key(self, cache: ZeroCache):
        assert cache.hget("nope", "field") is None

    def test_hset_multiple_fields(self, cache: ZeroCache):
        cache.hset("user:1", "name", "Alice")
        cache.hset("user:1", "age", 30)
        assert cache.hget("user:1", "name") == "Alice"
        assert cache.hget("user:1", "age") == 30


class TestHmset:
    def test_hmset_creates_hash(self, cache: ZeroCache):
        cache.hmset("user:1", {"name": "Bob", "age": 25})
        assert cache.hget("user:1", "name") == "Bob"
        assert cache.hget("user:1", "age") == 25

    def test_hmset_updates_existing(self, cache: ZeroCache):
        cache.hmset("user:1", {"name": "Bob"})
        cache.hmset("user:1", {"age": 25})
        assert cache.hget("user:1", "name") == "Bob"
        assert cache.hget("user:1", "age") == 25

    def test_hmget(self, cache: ZeroCache):
        cache.hmset("user:1", {"name": "Carol", "age": 22, "city": "NY"})
        vals = cache.hmget("user:1", "name", "age", "missing")
        assert vals == ["Carol", 22, None]


class TestHgetall:
    def test_hgetall(self, cache: ZeroCache):
        cache.hmset("user:1", {"name": "Dave", "age": 40})
        result = cache.hgetall("user:1")
        assert result == {"name": "Dave", "age": 40}

    def test_hgetall_missing_key(self, cache: ZeroCache):
        assert cache.hgetall("nope") == {}


class TestHkeysValsLen:
    def test_hkeys(self, cache: ZeroCache):
        cache.hmset("h", {"a": 1, "b": 2})
        assert set(cache.hkeys("h")) == {"a", "b"}

    def test_hvals(self, cache: ZeroCache):
        cache.hmset("h", {"a": 1, "b": 2})
        assert set(cache.hvals("h")) == {1, 2}

    def test_hlen(self, cache: ZeroCache):
        cache.hmset("h", {"a": 1, "b": 2, "c": 3})
        assert cache.hlen("h") == 3

    def test_hlen_missing(self, cache: ZeroCache):
        assert cache.hlen("nope") == 0


class TestHexists:
    def test_hexists_true(self, cache: ZeroCache):
        cache.hset("h", "field", "val")
        assert cache.hexists("h", "field") is True

    def test_hexists_false(self, cache: ZeroCache):
        cache.hset("h", "field", "val")
        assert cache.hexists("h", "other") is False


class TestHdel:
    def test_hdel_removes_field(self, cache: ZeroCache):
        cache.hmset("h", {"a": 1, "b": 2})
        removed = cache.hdel("h", "a")
        assert removed == 1
        assert cache.hget("h", "a") is None
        assert cache.hget("h", "b") == 2

    def test_hdel_multiple(self, cache: ZeroCache):
        cache.hmset("h", {"a": 1, "b": 2, "c": 3})
        assert cache.hdel("h", "a", "b", "nope") == 2

    def test_hdel_missing_key(self, cache: ZeroCache):
        assert cache.hdel("nope", "field") == 0


class TestHincrby:
    def test_hincrby_creates_field(self, cache: ZeroCache):
        result = cache.hincrby("h", "counter")
        assert result == 1

    def test_hincrby_increments(self, cache: ZeroCache):
        cache.hset("h", "counter", 10)
        assert cache.hincrby("h", "counter", 5) == 15

    def test_hincrby_negative(self, cache: ZeroCache):
        cache.hset("h", "counter", 10)
        assert cache.hincrby("h", "counter", -3) == 7
