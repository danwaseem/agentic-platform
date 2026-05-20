"""FastAPI router: POST /search with Redis cache and mock results."""

import hashlib
import json
import logging
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from db.redis_client import get_redis_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])

_CACHE_TTL = 3600  # seconds


# ------------------------------------------------------------------ #
# Request / response models
# ------------------------------------------------------------------ #

class SearchRequest(BaseModel):
    query: str


class SearchResult(BaseModel):
    title: str
    score: float


class SearchResponse(BaseModel):
    query: str
    search_id: str
    results: list[SearchResult]
    cache_hit: bool


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _cache_key(query: str) -> str:
    digest = hashlib.sha256(query.strip().lower().encode()).hexdigest()
    return f"search:{digest}"


def _mock_results(query: str) -> list[dict[str, Any]]:
    """Return deterministic mock search results for demo purposes."""
    return [
        {"title": f"Result for '{query}'", "score": 0.95},
        {"title": f"Related: {query} — overview", "score": 0.82},
        {"title": f"Deep dive into {query}", "score": 0.74},
    ]


# ------------------------------------------------------------------ #
# Endpoint
# ------------------------------------------------------------------ #

@router.post("", response_model=SearchResponse)
def search(request: SearchRequest) -> SearchResponse:
    """Search for documents matching the query.

    Checks Redis for a cached response first. On a miss, generates mock
    results (swap in a real vector/full-text search in production), caches
    them, and returns them with cache_hit=False.

    Args:
        request: Body containing the search query string.

    Returns:
        SearchResponse with results and a cache flag.
    """
    import uuid
    query = request.query.strip()
    cache_key = _cache_key(query)
    redis = get_redis_client()
    search_id = str(uuid.uuid4())

    logger.info("POST /search query=%r", query)

    # ---- Cache check ------------------------------------------------
    cached = redis.get(cache_key)
    if cached:
        results = json.loads(cached)
        logger.info("Cache HIT for query=%r", query)
        return SearchResponse(
            query=query,
            search_id=search_id,
            results=[SearchResult(**r) for r in results],
            cache_hit=True,
        )

    # ---- Mock search ------------------------------------------------
    raw_results = _mock_results(query)
    redis.setex(cache_key, _CACHE_TTL, json.dumps(raw_results))
    logger.info("Cache MISS — stored %d results for query=%r", len(raw_results), query)

    return SearchResponse(
        query=query,
        search_id=search_id,
        results=[SearchResult(**r) for r in raw_results],
        cache_hit=False,
    )
