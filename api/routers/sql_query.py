"""FastAPI router: POST /sql/query — text-to-SQL with Redis caching."""

import json
import logging
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db.mysql_client import execute_select_query
from text_to_sql.cache import RedisCache
from text_to_sql.generator import TextToSQLGenerator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sql", tags=["sql"])

# Load the schema DDL once at import time so it can be injected into prompts.
_SCHEMA_PATH = Path(__file__).parent.parent.parent / "db" / "schema.sql"
_SCHEMA_DDL = _SCHEMA_PATH.read_text() if _SCHEMA_PATH.exists() else ""

_cache = RedisCache()
_generator = TextToSQLGenerator(mysql_schema=_SCHEMA_DDL)


# ------------------------------------------------------------------ #
# Request / response models
# ------------------------------------------------------------------ #

class SQLQueryRequest(BaseModel):
    question: str


class SQLQueryResponse(BaseModel):
    question: str
    sql: str
    results: list[dict[str, Any]]
    cache_hit: bool
    latency_ms: float


# ------------------------------------------------------------------ #
# Endpoint
# ------------------------------------------------------------------ #

@router.post("/query", response_model=SQLQueryResponse)
def query(request: SQLQueryRequest) -> SQLQueryResponse:
    """Convert a natural-language question to SQL and execute it.

    Checks Redis for a cached response before calling the LLM. On a cache
    miss, generates SQL via Ollama (with deterministic fallback), runs it
    against MySQL, caches the result, and returns it.

    Args:
        request: Body containing the natural-language question.

    Returns:
        SQLQueryResponse with the generated SQL, result rows, cache flag,
        and round-trip latency.
    """
    start = time.perf_counter()
    question = request.question.strip()
    cache_key = _cache.make_key(question)

    logger.info("POST /sql/query question=%r", question)

    # ---- Cache check ------------------------------------------------
    cached = _cache.get(cache_key)
    if cached:
        try:
            payload = json.loads(cached)
            latency_ms = (time.perf_counter() - start) * 1000
            logger.info("Cache HIT latency_ms=%.1f", latency_ms)
            return SQLQueryResponse(
                question=question,
                sql=payload["sql"],
                results=payload["results"],
                cache_hit=True,
                latency_ms=round(latency_ms, 2),
            )
        except (json.JSONDecodeError, KeyError):
            # Stale or malformed cache entry — treat as a miss and overwrite
            logger.warning("Invalid cache entry for key=%s — treating as miss", cache_key[:20])

    # ---- Generate SQL -----------------------------------------------
    try:
        sql = _generator.generate(question)
    except ValueError as exc:
        logger.error("SQL generation failed: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # ---- Execute ----------------------------------------------------
    try:
        results = execute_select_query(sql)
    except ValueError as exc:
        logger.error("Unsafe SQL rejected: %s", exc)
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("MySQL execution error: %s", exc)
        raise HTTPException(status_code=500, detail=f"Query execution failed: {exc}") from exc

    # ---- Cache store ------------------------------------------------
    _cache.set(cache_key, json.dumps({"sql": sql, "results": results}, default=str))

    latency_ms = (time.perf_counter() - start) * 1000
    logger.info("Cache MISS sql=%r rows=%d latency_ms=%.1f", sql[:80], len(results), latency_ms)

    return SQLQueryResponse(
        question=question,
        sql=sql,
        results=results,
        cache_hit=False,
        latency_ms=round(latency_ms, 2),
    )
