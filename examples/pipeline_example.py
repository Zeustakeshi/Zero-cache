"""
Pipeline example — batch commands for zero round-trip overhead.
"""

from zerocache import ZeroCache


def main():
    cache = ZeroCache(load_on_start=False)

    # ── Basic pipeline ──────────────────────────────────────────────
    print("=== Basic Pipeline ===")
    results = (
        cache.pipeline()
        .set("user:1:name", "Alice")
        .set("user:1:age", 30)
        .set("user:2:name", "Bob")
        .set("user:2:age", 25)
        .get("user:1:name")
        .get("user:2:name")
        .execute()
    )
    print(f"Results: {results}")
    # → [True, True, True, True, "Alice", "Bob"]

    # ── Counter pipeline ────────────────────────────────────────────
    print("\n=== Counter Pipeline ===")
    counters = (
        cache.pipeline()
        .set("page_views", 0)
        .incr("page_views")
        .incr("page_views")
        .incr("page_views")
        .get("page_views")
        .execute()
    )
    print(f"Page views: {counters[-1]}")
    # → 3

    # ── Hash pipeline ───────────────────────────────────────────────
    print("\n=== Hash Pipeline ===")
    cache.pipeline().mset(
        {
            "product:1": {"name": "Widget", "price": 9.99},
            "product:2": {"name": "Gadget", "price": 24.99},
        }
    ).execute()
    print(f"Product 1: {cache.get('product:1')}")

    # ── Mixed operations ────────────────────────────────────────────
    print("\n=== Mixed Operations ===")
    mixed = (
        cache.pipeline()
        .rpush("queue", "job_a", "job_b")
        .sadd("tags", "python", "cache")
        .zadd("scores", {"alice": 100, "bob": 85})
        .expire("queue", 60)
        .execute()
    )
    print(f"Queue length: {mixed[0]}")
    print(f"Tags added: {mixed[1]}")

    print(f"\nCache info: {cache.info()}")
    cache.shutdown()


if __name__ == "__main__":
    main()
