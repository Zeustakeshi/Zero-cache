"""Tests for core ZeroCache operations: get/set/delete/exists/expire/ttl/flush."""

from __future__ import annotations

import time
import pytest
from zerocache import ZeroCache


class TestSetGet:
    def test_set_and_get(self, cache: ZeroCache):
        cache.set("k", "hello")
        assert cache.get("k") == "hello"

    def test_get_missing_returns_default(self, cache: ZeroCache):
        assert cache.get("nope") is None
        assert cache.get("nope", "fallback") == "fallback"

    def test_set_overwrite(self, cache: ZeroCache):
        cache.set("k", 1)
        cache.set("k", 2)
        assert cache.get("k") == 2

    def test_set_nx_success(self, cache: ZeroCache):
        assert cache.set("k", "a", nx=True) is True
        assert cache.get("k") == "a"

    def test_set_nx_fails_if_exists(self, cache: ZeroCache):
        cache.set("k", "a")
        assert cache.set("k", "b", nx=True) is False
        assert cache.get("k") == "a"

    def test_set_xx_success(self, cache: ZeroCache):
        cache.set("k", "a")
        assert cache.set("k", "b", xx=True) is True
        assert cache.get("k") == "b"

    def test_set_xx_fails_if_missing(self, cache: ZeroCache):
        assert cache.set("k", "b", xx=True) is False
        assert cache.get("k") is None

    def test_set_various_types(self, cache: ZeroCache):
        cache.set("int",   42)
        cache.set("float", 3.14)
        cache.set("list",  [1, 2, 3])
        cache.set("dict",  {"a": 1})
        assert cache.get("int")   == 42
        assert cache.get("float") == 3.14
        assert cache.get("list")  == [1, 2, 3]
        assert cache.get("dict")  == {"a": 1}


class TestDelete:
    def test_delete_existing(self, cache: ZeroCache):
        cache.set("k", "v")
        assert cache.delete("k") == 1
        assert cache.get("k") is None

    def test_delete_missing_returns_zero(self, cache: ZeroCache):
        assert cache.delete("nope") == 0

    def test_delete_multiple(self, cache: ZeroCache):
        cache.set("a", 1); cache.set("b", 2)
        assert cache.delete("a", "b", "nope") == 2


class TestExists:
    def test_exists_present(self, cache: ZeroCache):
        cache.set("k", "v")
        assert cache.exists("k") == 1

    def test_exists_missing(self, cache: ZeroCache):
        assert cache.exists("nope") == 0

    def test_exists_multiple(self, cache: ZeroCache):
        cache.set("a", 1); cache.set("b", 2)
        assert cache.exists("a", "b", "nope") == 2

    def test_contains_operator(self, cache: ZeroCache):
        cache.set("k", "v")
        assert "k" in cache
        assert "nope" not in cache


class TestTTL:
    def test_set_with_ttl_accessible_before_expiry(self, cache: ZeroCache):
        cache.set("k", "v", ttl=60)
        assert cache.get("k") == "v"

    def test_set_with_short_ttl_expires(self, cache: ZeroCache):
        cache.set("k", "v", ttl=1)
        time.sleep(1.1)
        assert cache.get("k") is None

    def test_ttl_immortal(self, cache: ZeroCache):
        cache.set("k", "v")
        assert cache.ttl("k") == -1

    def test_ttl_with_expiry(self, cache: ZeroCache):
        cache.set("k", "v", ttl=60)
        remaining = cache.ttl("k")
        assert 58 <= remaining <= 60

    def test_ttl_missing_key(self, cache: ZeroCache):
        assert cache.ttl("nope") == -2

    def test_expire_updates_ttl(self, cache: ZeroCache):
        cache.set("k", "v")
        result = cache.expire("k", 120)
        assert result is True
        assert 118 <= cache.ttl("k") <= 120

    def test_expire_missing_key(self, cache: ZeroCache):
        assert cache.expire("nope", 60) is False

    def test_persist_removes_ttl(self, cache: ZeroCache):
        cache.set("k", "v", ttl=60)
        cache.persist("k")
        assert cache.ttl("k") == -1


class TestType:
    def test_type_string(self, cache: ZeroCache):
        cache.set("k", "hello")
        assert cache.type("k") == "string"

    def test_type_hash(self, cache: ZeroCache):
        cache.hset("k", "f", "v")
        assert cache.type("k") == "hash"

    def test_type_list(self, cache: ZeroCache):
        cache.rpush("k", 1)
        assert cache.type("k") == "list"

    def test_type_set(self, cache: ZeroCache):
        cache.sadd("k", "a")
        assert cache.type("k") == "set"

    def test_type_none(self, cache: ZeroCache):
        assert cache.type("nope") == "none"


class TestFlushKeysRename:
    def test_flush_clears_all(self, cache: ZeroCache):
        cache.set("a", 1); cache.set("b", 2)
        cache.flush()
        assert len(cache) == 0

    def test_keys_all(self, cache: ZeroCache):
        cache.set("foo", 1); cache.set("bar", 2)
        ks = cache.keys()
        assert set(ks) == {"foo", "bar"}

    def test_keys_pattern(self, cache: ZeroCache):
        cache.set("user:1", 1); cache.set("user:2", 2); cache.set("item:1", 3)
        ks = cache.keys("user:*")
        assert set(ks) == {"user:1", "user:2"}

    def test_scan_iter(self, cache: ZeroCache):
        cache.set("a", 1); cache.set("b", 2)
        found = list(cache.scan_iter("*"))
        assert set(found) == {"a", "b"}

    def test_rename(self, cache: ZeroCache):
        cache.set("src", "value")
        result = cache.rename("src", "dst")
        assert result is True
        assert cache.get("src") is None
        assert cache.get("dst") == "value"

    def test_rename_same_key(self, cache: ZeroCache):
        cache.set("k", "v")
        assert cache.rename("k", "k") is True
        assert cache.get("k") == "v"

    def test_rename_missing(self, cache: ZeroCache):
        assert cache.rename("nope", "dst") is False


class TestStringOps:
    def test_incr(self, cache: ZeroCache):
        assert cache.incr("counter") == 1
        assert cache.incr("counter") == 2
        assert cache.incr("counter", 5) == 7

    def test_decr(self, cache: ZeroCache):
        cache.set("counter", 10)
        assert cache.decr("counter") == 9
        assert cache.decr("counter", 4) == 5

    def test_append(self, cache: ZeroCache):
        cache.set("k", "hello")
        length = cache.append("k", " world")
        assert length == 11
        assert cache.get("k") == "hello world"

    def test_getset(self, cache: ZeroCache):
        cache.set("k", "old")
        old = cache.getset("k", "new")
        assert old == "old"
        assert cache.get("k") == "new"

    def test_mget_mset(self, cache: ZeroCache):
        cache.mset({"a": 1, "b": 2, "c": 3})
        vals = cache.mget("a", "b", "c", "nope")
        assert vals == [1, 2, 3, None]


class TestInfo:
    def test_info_keys(self, cache: ZeroCache):
        cache.set("k", "v")
        info = cache.info()
        assert info["name"] == "ZeroCache"
        assert info["version"] == "1.1.1"
        assert info["keys"] == 1
