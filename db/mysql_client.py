"""MySQL client: SQLAlchemy engine factory and safe SELECT execution."""

import logging
import re
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Words that must never appear in an allowed query
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|replace|grant|revoke)\b",
    re.IGNORECASE,
)

_DEFAULT_URL = "mysql+pymysql://danish:danish123@127.0.0.1:3307/agentdb"


def get_engine(database_url: str = _DEFAULT_URL) -> Engine:
    """Return a SQLAlchemy engine for the given database URL.

    Args:
        database_url: SQLAlchemy-compatible connection string.

    Returns:
        A configured Engine instance.
    """
    engine = create_engine(database_url, pool_pre_ping=True)
    logger.debug("SQLAlchemy engine created for %s", database_url)
    return engine


def execute_select_query(
    sql: str,
    database_url: str = _DEFAULT_URL,
) -> list[dict[str, Any]]:
    """Execute a read-only SELECT query and return results as a list of dicts.

    Only SELECT statements are permitted. Any SQL containing DML/DDL keywords
    (INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, etc.) is rejected before
    reaching the database.

    Args:
        sql: A SQL SELECT statement.
        database_url: SQLAlchemy connection string (defaults to local MySQL).

    Returns:
        A list of row dicts, one per result row.

    Raises:
        ValueError: If the query is not a SELECT or contains forbidden keywords.
    """
    normalised = sql.strip()

    if not normalised.upper().startswith("SELECT"):
        raise ValueError(f"Only SELECT queries are allowed. Got: {normalised[:80]}")

    if _FORBIDDEN.search(normalised):
        raise ValueError(f"Query contains forbidden keyword: {normalised[:80]}")

    engine = get_engine(database_url)
    with engine.connect() as conn:
        result = conn.execute(text(normalised))
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

    logger.debug("execute_select_query returned %d row(s)", len(rows))
    return rows
