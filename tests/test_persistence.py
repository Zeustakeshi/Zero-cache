"""Tests for persistence: save/load snapshot to/from disk."""

from __future__ import annotations

from zerocache import ZeroCache


class TestSaveLoad:
    def test_save_and_load(self, tmp_path):
        db_path = str(tmp_path / "test.db")

        # Write data to first instance
        c1 = ZeroCache(persist_path=db_path, load_on_start=False, auto_save_interval=9999)
        c1.set("key1", "value1")
        c1.set("key2", {"nested": True})
        c1.save()
        c1.shutdown()

        # Load from second instance
        c2 = ZeroCache(persist_path=db_path, load_on_start=True, auto_save_interval=9999)
        assert c2.get("key1") == "value1"
        assert c2.get("key2") == {"nested": True}
        c2.shutdown()

    def test_save_skips_expired_keys(self, tmp_path):
        import time

        db_path = str(tmp_path / "test.db")

        c1 = ZeroCache(persist_path=db_path, load_on_start=False, auto_save_interval=9999)
        c1.set("immortal", "stays")
        c1.set("ephemeral", "gone", ttl=1)
        time.sleep(1.1)
        c1.save()
        c1.shutdown()

        c2 = ZeroCache(persist_path=db_path, load_on_start=True, auto_save_interval=9999)
        assert c2.get("immortal") == "stays"
        assert c2.get("ephemeral") is None
        c2.shutdown()

    def test_load_nonexistent_file(self, tmp_path):
        """Loading from a missing file should not raise."""
        db_path = str(tmp_path / "missing.db")
        c = ZeroCache(persist_path=db_path, load_on_start=True, auto_save_interval=9999)
        assert len(c) == 0
        c.shutdown()

    def test_compression_roundtrip(self, tmp_path):
        db_path = str(tmp_path / "compressed.db")

        c1 = ZeroCache(
            persist_path=db_path,
            load_on_start=False,
            compress=True,
            auto_save_interval=9999,
        )
        for i in range(100):
            c1.set(f"key:{i}", f"value:{i}")
        c1.save()
        c1.shutdown()

        c2 = ZeroCache(
            persist_path=db_path,
            load_on_start=True,
            compress=True,
            auto_save_interval=9999,
        )
        assert len(c2) == 100
        assert c2.get("key:42") == "value:42"
        c2.shutdown()

    def test_info_save_count(self, cache: ZeroCache):
        cache.set("k", "v")
        cache.save()
        assert cache.info()["saves"] >= 1
