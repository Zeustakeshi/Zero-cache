"""Tests for Pub/Sub: publish/subscribe/unsubscribe."""

from __future__ import annotations

import asyncio
import pytest
from zerocache import ZeroCache


class TestPubSub:
    async def test_publish_subscribe(self, cache: ZeroCache):
        queue = await cache.subscribe("channel")
        recipients = await cache.publish("channel", "hello")

        assert recipients == 1
        msg = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert msg == "hello"

    async def test_multiple_subscribers(self, cache: ZeroCache):
        q1 = await cache.subscribe("chan")
        q2 = await cache.subscribe("chan")

        recipients = await cache.publish("chan", "broadcast")
        assert recipients == 2

        m1 = await asyncio.wait_for(q1.get(), timeout=1.0)
        m2 = await asyncio.wait_for(q2.get(), timeout=1.0)
        assert m1 == m2 == "broadcast"

    async def test_publish_no_subscribers(self, cache: ZeroCache):
        recipients = await cache.publish("empty_channel", "msg")
        assert recipients == 0

    async def test_unsubscribe(self, cache: ZeroCache):
        queue = await cache.subscribe("chan")
        cache.unsubscribe("chan", queue)

        recipients = await cache.publish("chan", "after_unsub")
        assert recipients == 0

    async def test_different_channels_isolated(self, cache: ZeroCache):
        q1 = await cache.subscribe("chan_a")
        await cache.publish("chan_b", "msg_b")

        # q1 should not receive messages from chan_b
        assert q1.empty()

    async def test_publish_various_types(self, cache: ZeroCache):
        queue = await cache.subscribe("typed")
        await cache.publish("typed", {"key": "value"})
        msg = await asyncio.wait_for(queue.get(), timeout=1.0)
        assert msg == {"key": "value"}
