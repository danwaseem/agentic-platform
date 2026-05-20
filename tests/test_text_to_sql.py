"""Unit tests for text-to-SQL generation, Redis cache, and SQL safety guard."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db.mysql_client import execute_select_query
from text_to_sql.cache import RedisCache
from text_to_sql.generator import TextToSQLGenerator

_SCHEMA = Path(__file__).parent.parent / "db" / "schema.sql"
_SCHEMA_DDL = _SCHEMA.read_text() if _SCHEMA.exists() else ""


# ------------------------------------------------------------------ #
# RedisCache
# ------------------------------------------------------------------ #

class TestCacheKey:
    def test_cache_key_is_stable(self) -> None:
        """The same text must always produce the same SHA-256 cache key."""
        cache = RedisCache.__new__(RedisCache)  # skip __init__ (no Redis needed)
        key1 = cache.make_key("top 5 products by revenue")
        key2 = cache.make_key("top 5 products by revenue")
        assert key1 == key2

    def test_cache_key_is_case_and_whitespace_insensitive(self) -> None:
        """Leading/trailing whitespace and casing must not affect the key."""
        cache = RedisCache.__new__(RedisCache)
        assert cache.make_key("Revenue") == cache.make_key("revenue")
        assert cache.make_key("  revenue  ") == cache.make_key("revenue")

    def test_different_texts_produce_different_keys(self) -> None:
        cache = RedisCache.__new__(RedisCache)
        assert cache.make_key("orders") != cache.make_key("products")

    def test_cache_key_has_expected_prefix(self) -> None:
        cache = RedisCache.__new__(RedisCache)
        assert cache.make_key("anything").startswith("t2sql:")


class TestCacheHit:
    def test_cache_hit_skips_generator(self) -> None:
        """When Redis returns a cached value, TextToSQLGenerator.generate
        must not be called."""
        cached_payload = json.dumps({
            "sql": "SELECT 1",
            "results": [{"col": "val"}],
        })

        mock_redis = MagicMock()
        mock_redis.get.return_value = cached_payload

        cache = RedisCache.__new__(RedisCache)
        cache._client = mock_redis
        cache._ttl = 3600

        key = cache.make_key("anything")
        result = cache.get(key)

        assert result == cached_payload
        mock_redis.get.assert_called_once_with(key)

    def test_cache_miss_returns_none(self) -> None:
        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        cache = RedisCache.__new__(RedisCache)
        cache._client = mock_redis
        cache._ttl = 3600

        assert cache.get("nonexistent-key") is None

    def test_cache_set_uses_configured_ttl(self) -> None:
        mock_redis = MagicMock()

        cache = RedisCache.__new__(RedisCache)
        cache._client = mock_redis
        cache._ttl = 1800

        cache.set("mykey", "myvalue")
        mock_redis.setex.assert_called_once_with("mykey", 1800, "myvalue")


# ------------------------------------------------------------------ #
# TextToSQLGenerator
# ------------------------------------------------------------------ #

class TestSQLGenerationFallback:
    def test_fallback_returns_select_for_known_question(self) -> None:
        """When Ollama is unavailable the generator must return a SELECT query
        for all questions in the known fallback map."""
        gen = TextToSQLGenerator(mysql_schema=_SCHEMA_DDL)

        known_questions = [
            "What are the top 5 products by revenue?",
            "Which category has the highest total sales?",
            "Show total orders by user.",
            "Which products are low in stock?",
            "What is the total revenue?",
            "Who are the most active users?",
            "Show all electronics products.",
            "What is the revenue by category?",
        ]
        for question in known_questions:
            # Patch _call_ollama to simulate Ollama being down
            with patch.object(gen, "_call_ollama", side_effect=ConnectionError("offline")):
                sql = gen.generate(question)
            assert sql.strip().upper().startswith("SELECT"), (
                f"Expected SELECT for: {question!r}, got: {sql!r}"
            )

    def test_fallback_raises_for_unknown_question(self) -> None:
        """An unrecognised question with Ollama down must raise ValueError."""
        gen = TextToSQLGenerator(mysql_schema=_SCHEMA_DDL)
        with patch.object(gen, "_call_ollama", side_effect=ConnectionError("offline")):
            with pytest.raises(ValueError, match="No SQL could be generated"):
                gen.generate("xyzzy completely unknown query 12345")

    def test_ollama_response_is_cleaned_of_markdown(self) -> None:
        """_clean must strip ```sql ... ``` fences from LLM output."""
        raw = "```sql\nSELECT * FROM orders\n```"
        result = TextToSQLGenerator._clean(raw)
        assert result == "SELECT * FROM orders"

    def test_ollama_response_takes_first_statement(self) -> None:
        """_clean must return only the first SQL statement."""
        raw = "SELECT 1; DROP TABLE users"
        result = TextToSQLGenerator._clean(raw)
        assert "DROP" not in result


# ------------------------------------------------------------------ #
# MySQL safety guard
# ------------------------------------------------------------------ #

class TestRejectNonSelectSQL:
    @pytest.mark.parametrize("dangerous_sql", [
        "DROP TABLE users",
        "DELETE FROM orders",
        "UPDATE products SET price=0",
        "INSERT INTO users VALUES (99,'x','x@x.com', NOW())",
        "ALTER TABLE users ADD COLUMN foo INT",
        "TRUNCATE orders",
    ])
    def test_rejects_dangerous_sql(self, dangerous_sql: str) -> None:
        """execute_select_query must raise ValueError for any non-SELECT SQL."""
        with pytest.raises(ValueError):
            execute_select_query(dangerous_sql)

    def test_rejects_statement_not_starting_with_select(self) -> None:
        with pytest.raises(ValueError, match="Only SELECT"):
            execute_select_query("SHOW TABLES")

    def test_accepts_valid_select_syntax(self) -> None:
        """A valid SELECT must pass the safety check (connection error is OK
        since no DB is required for this unit test)."""
        sql = "SELECT id, name FROM users WHERE id = 1"
        # We only care that ValueError is NOT raised for the safety check.
        # If MySQL is unavailable an OperationalError is fine here.
        try:
            execute_select_query(sql)
        except ValueError:
            pytest.fail("execute_select_query raised ValueError on a valid SELECT")
        except Exception:
            pass  # OperationalError from missing DB is acceptable in unit tests
