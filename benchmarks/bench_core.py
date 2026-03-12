"""
Benchmark: ZeroCache get/set throughput.

Usage:
    pip install zero-cache
    python benchmarks/bench_core.py
"""

from __future__ import annotations

import time
from zerocache import ZeroCache


def bench(label: str, fn, iterations: int = 100_000) -> None:
    # Warmup
    for _ in range(1000):
        fn()

    start = time.perf_counter()
    for _ in range(iterations):
        fn()
    elapsed = time.perf_counter() - start
    ops_per_sec = iterations / elapsed
    print(f"  {label:<30} {ops_per_sec:>12,.0f} ops/sec   ({elapsed*1000:.1f} ms)")


def main():
    cache = ZeroCache(load_on_start=False, track_hits=False, intern_keys=True)

    # Pre-fill for get benchmark
    for i in range(1000):
        cache.set(f"key:{i}", f"value:{i}")

    print(f"\nZeroCache v{cache.VERSION} — Benchmark ({100_000:,} iterations)\n")

    bench("set(str, str)",        lambda: cache.set("bench:str", "hello"))
    bench("set(str, int)",        lambda: cache.set("bench:int", 42))
    bench("get(hit)",             lambda: cache.get("key:500"))
    bench("get(miss)",            lambda: cache.get("no_such_key"))
    bench("set(ttl=60)",          lambda: cache.set("bench:ttl", "v", ttl=60))
    bench("incr",                 lambda: cache.incr("bench:counter"))
    bench("hset",                 lambda: cache.hset("bench:hash", "field", "val"))
    bench("hget",                 lambda: cache.hget("bench:hash", "field"))
    bench("rpush",                lambda: cache.rpush("bench:list", "item"))
    bench("sadd",                 lambda: cache.sadd("bench:set", "member"))
    bench("zadd",                 lambda: cache.zadd("bench:zset", {"member": 1.0}))

    print()
    cache.shutdown()


if __name__ == "__main__":
    main()
