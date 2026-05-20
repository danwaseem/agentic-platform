"""FastAPI application entry point."""

import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.routers import tasks, search, sql_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

APP_VERSION = "0.1.0"


# ------------------------------------------------------------------ #
# Lifespan
# ------------------------------------------------------------------ #

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Agentic Platform API starting (version=%s)", APP_VERSION)
    yield
    logger.info("Agentic Platform API stopped")


# ------------------------------------------------------------------ #
# App
# ------------------------------------------------------------------ #

app = FastAPI(
    title="Agentic Data Platform API",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------ #
# Request logging middleware
# ------------------------------------------------------------------ #

@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    """Log method, path, status code, and response time for every request."""
    start = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "%s %s → %d (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ------------------------------------------------------------------ #
# Routers
# ------------------------------------------------------------------ #

app.include_router(tasks.router)
app.include_router(search.router)
app.include_router(sql_query.router)


# ------------------------------------------------------------------ #
# Health
# ------------------------------------------------------------------ #

@app.get("/health", tags=["meta"])
def health() -> dict:
    """Return API liveness status."""
    return {
        "status": "ok",
        "version": APP_VERSION,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }
