"""
FastAPI example using ZeroCache as a dependency injection cache.

Usage:
    pip install fastapi uvicorn zero-cache
    uvicorn examples.fastapi_example:app --reload
"""

from fastapi import Depends, FastAPI

from zerocache import ZeroCache, get_cache

app = FastAPI(title="ZeroCache + FastAPI Example")


@app.get("/items/{item_id}")
async def read_item(item_id: int, cache: ZeroCache = Depends(get_cache)):
    """Return item from cache or compute and store it."""
    cached = cache.get(f"item:{item_id}")
    if cached:
        return {"source": "cache", "data": cached}

    # Simulate DB fetch
    result = {"id": item_id, "name": f"Widget #{item_id}"}
    cache.set(f"item:{item_id}", result, ttl=60)
    return {"source": "computed", "data": result}


@app.delete("/items/{item_id}/cache")
async def invalidate_item(item_id: int, cache: ZeroCache = Depends(get_cache)):
    """Manually invalidate cached item."""
    deleted = cache.delete(f"item:{item_id}")
    return {"invalidated": deleted > 0}


@app.get("/cache/info")
async def cache_info(cache: ZeroCache = Depends(get_cache)):
    """Return cache runtime statistics."""
    return cache.info()
