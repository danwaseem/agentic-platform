"""Text-to-SQL generator using Ollama as the local LLM backend."""

import logging
import re

import httpx

logger = logging.getLogger(__name__)

# Fallback queries for common demo questions when Ollama is unavailable.
# Keys are lowercase substrings to match against the natural-language query.
_FALLBACK_MAP: dict[str, str] = {
    "top 5 products by revenue": (
        "SELECT p.name, SUM(o.total) AS revenue "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY p.id, p.name ORDER BY revenue DESC LIMIT 5"
    ),
    "top products by revenue": (
        "SELECT p.name, SUM(o.total) AS revenue "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY p.id, p.name ORDER BY revenue DESC LIMIT 5"
    ),
    "total revenue": (
        "SELECT SUM(total) AS total_revenue FROM orders"
    ),
    "most orders": (
        "SELECT u.name, COUNT(o.id) AS order_count "
        "FROM orders o JOIN users u ON o.user_id = u.id "
        "GROUP BY u.id, u.name ORDER BY order_count DESC LIMIT 5"
    ),
    "most active user": (
        "SELECT u.name, COUNT(o.id) AS order_count "
        "FROM orders o JOIN users u ON o.user_id = u.id "
        "GROUP BY u.id, u.name ORDER BY order_count DESC LIMIT 1"
    ),
    "all users": (
        "SELECT id, name, email, created_at FROM users ORDER BY id"
    ),
    "all products": (
        "SELECT id, name, category, price, stock FROM products ORDER BY id"
    ),
    "orders": (
        "SELECT o.id, u.name AS user, p.name AS product, o.quantity, o.total, o.ordered_at "
        "FROM orders o "
        "JOIN users u ON o.user_id = u.id "
        "JOIN products p ON o.product_id = p.id "
        "ORDER BY o.ordered_at DESC LIMIT 20"
    ),
    "low stock": (
        "SELECT name, category, stock FROM products WHERE stock < 50 ORDER BY stock ASC"
    ),
    "low in stock": (
        "SELECT name, category, stock FROM products WHERE stock < 50 ORDER BY stock ASC"
    ),
    "electronics": (
        "SELECT id, name, price, stock FROM products WHERE category = 'Electronics' ORDER BY price DESC"
    ),
    "revenue by category": (
        "SELECT p.category, SUM(o.total) AS revenue "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY p.category ORDER BY revenue DESC"
    ),
    "highest total sales": (
        "SELECT p.category, SUM(o.total) AS total_sales "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY p.category ORDER BY total_sales DESC LIMIT 1"
    ),
    "total sales": (
        "SELECT p.category, SUM(o.total) AS total_sales "
        "FROM orders o JOIN products p ON o.product_id = p.id "
        "GROUP BY p.category ORDER BY total_sales DESC"
    ),
}

_PROMPT_TEMPLATE = """\
You are a SQL expert. Given the following MySQL schema, write a safe SELECT query.

Schema:
{schema}

Question: {question}

Rules:
- Return only the SQL statement, nothing else.
- Do not include markdown, backticks, or explanations.
- Only use SELECT — no INSERT, UPDATE, DELETE, DROP, or ALTER.
- Use JOINs when the question spans multiple tables.

SQL:"""


class TextToSQLGenerator:
    """Convert natural-language questions to MySQL SELECT queries via Ollama.

    Falls back to a hardcoded query map when Ollama is unreachable, so the
    API remains usable during demos without a running LLM.

    Args:
        ollama_base_url: Base URL of the Ollama HTTP API.
        model_name: Ollama model identifier (e.g. 'llama3').
        mysql_schema: Full DDL of the target database, injected into the prompt.
    """

    def __init__(
        self,
        ollama_base_url: str = "http://localhost:11434",
        model_name: str = "llama3",
        mysql_schema: str = "",
    ) -> None:
        self._base_url = ollama_base_url.rstrip("/")
        self._model = model_name
        self._schema = mysql_schema

    def generate(self, natural_language_query: str) -> str:
        """Generate a SELECT SQL query for the given natural-language question.

        Tries Ollama first; falls back to the deterministic map if the LLM is
        unavailable or returns an unusable response.

        Args:
            natural_language_query: The user's question in plain English.

        Returns:
            A SQL SELECT statement string.

        Raises:
            ValueError: If neither Ollama nor the fallback map can produce a
                        valid SELECT query.
        """
        logger.info("Generating SQL for: %r", natural_language_query)

        try:
            sql = self._call_ollama(natural_language_query)
            sql = self._clean(sql)
            if sql.upper().startswith("SELECT"):
                logger.info("Ollama produced SQL: %s", sql[:120])
                return sql
            logger.warning("Ollama response did not start with SELECT — using fallback")
        except Exception as exc:
            logger.warning("Ollama unavailable (%s) — using fallback map", exc)

        return self._fallback(natural_language_query)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _call_ollama(self, question: str) -> str:
        """POST to Ollama /api/generate and return the raw response text."""
        prompt = _PROMPT_TEMPLATE.format(schema=self._schema, question=question)
        response = httpx.post(
            f"{self._base_url}/api/generate",
            json={"model": self._model, "prompt": prompt, "stream": False},
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()["response"]

    @staticmethod
    def _clean(raw: str) -> str:
        """Strip markdown code fences and excess whitespace from LLM output."""
        # Remove ```sql ... ``` or ``` ... ``` blocks
        raw = re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE)
        raw = raw.replace("`", "").strip()
        # Take only the first statement if multiple are returned
        first_stmt = raw.split(";")[0].strip()
        return first_stmt

    def _fallback(self, question: str) -> str:
        """Return a hardcoded query for known demo questions.

        Raises:
            ValueError: If the question does not match any fallback entry.
        """
        lower = question.lower()
        for keyword, sql in _FALLBACK_MAP.items():
            if keyword in lower:
                logger.info("Fallback match for keyword %r", keyword)
                return sql
        raise ValueError(
            f"No SQL could be generated and no fallback matches: {question!r}"
        )
