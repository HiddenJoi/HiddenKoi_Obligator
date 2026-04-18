"""
Redis cache layer with MD5-based key generation and TTL support.
"""
import json
import hashlib
import logging

import os
from typing import Optional, Any

import redis.asyncio as redis

logger = logging.getLogger(__name__)

REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB       = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

_redis_client: Optional[redis.Redis] = None

async def init_redis() -> redis.Redis:
    global _redis_client
    _redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    await _redis_client.ping()
    logger.info("Redis connected: %s:%s db=%s", REDIS_HOST, REDIS_PORT, REDIS_DB)
    return _redis_client

async def close_redis():
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("Redis connection closed")

def get_redis() -> redis.Redis:
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() in lifespan.")
    return _redis_client

def _normalize_params(params: dict) -> dict:
    return {k: v for k, v in sorted(params.items()) if v is not None}

def make_cache_key(prefix: str, params: dict) -> str:
    normalized = _normalize_params(params)
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    digest = hashlib.md5(raw.encode()).hexdigest()
    return f"{prefix}:{digest}"

async def cache_get(key: str) -> Optional[str]:
    try:
        return await get_redis().get(key)
    except Exception as e:
        logger.warning("cache_get error [%s]: %s", key, e)
        return None

async def cache_set(key: str, value: str, ttl: int):
    try:
        await get_redis().setex(key, ttl, value)
    except Exception as e:
        logger.warning("cache_set error [%s]: %s", key, e)

async def cache_ttl(key: str) -> int:
    try:
        return await get_redis().ttl(key)
    except Exception:
        return -2

def _headers(hit: bool, ttl: int, remaining: int) -> dict:
    return {
        "X-Cache-Hit": "true" if hit else "false",
        "X-Cache-TTL":  str(max(remaining if hit else ttl, 0)),
    }

async def cached_get(prefix: str, request, exclude_pagination: bool = True):
    """
    Try to get cached response from Redis.
    Returns (cached_json_string, headers_dict) on hit, or (None, None) on miss/error.
    """
    try:
        client = get_redis()
    except RuntimeError:
        return None, None

    params = dict(request.query_params)
    if exclude_pagination:
        params = {k: v for k, v in params.items() if k not in ("limit", "offset")}

    key = make_cache_key(prefix, params)
    raw = await cache_get(key)

    if raw is not None:
        remaining = await cache_ttl(key)
        logger.debug("CACHE HIT %s ttl=%d", key, remaining)
        return raw, _headers(hit=True, ttl=0, remaining=remaining)

    logger.debug("CACHE MISS %s", key)
    return None, None

async def cached_set(key: str, data: Any, ttl: int):
    """Serialize data (Pydantic model or dict) and store with TTL."""
    try:
        if hasattr(data, "model_dump_json"):
            payload = data.model_dump_json()
        elif hasattr(data, "dict"):
            payload = json.dumps(data.dict(), default=str)
        else:
            payload = json.dumps(data, default=str)
        await cache_set(key, payload, ttl)
    except Exception as e:
        logger.warning("cache_set error [%s]: %s", key, e)

def make_key(prefix: str, request, exclude_pagination: bool = True) -> str:
    params = dict(request.query_params)
    if exclude_pagination:
        params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    return make_cache_key(prefix, params)
