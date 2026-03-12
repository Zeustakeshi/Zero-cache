"""Shared pytest fixtures for ZeroCache test suite."""

from __future__ import annotations

import pytest
from zerocache import ZeroCache


@pytest.fixture
def cache(tmp_path):
    """Provide a fresh ZeroCache instance per test.

    Uses a temp file for persistence so tests don't interfere with each other.
    load_on_start=False prevents reading stale state.
    """
    c = ZeroCache(
        persist_path=str(tmp_path / ".zerocache_test.db"),
        load_on_start=False,
        auto_save_interval=9999,   # disable auto-save during tests
    )
    yield c
    c.flush()
    c.shutdown()
