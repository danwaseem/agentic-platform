"""Locust load test: simulates read-heavy and write traffic against the API."""

import logging
import random

from locust import HttpUser, between, task

logger = logging.getLogger(__name__)

_SEARCH_QUERIES = [
    "revenue trends",
    "top selling products",
    "low stock items",
    "user order history",
    "electronics category",
    "monthly sales",
    "best customers",
    "product categories",
    "recent orders",
    "total revenue by product",
]

_SQL_QUESTIONS = [
    "What are the top 5 products by revenue?",
    "Which category has the highest total sales?",
    "Show total orders by user.",
    "Which products are low in stock?",
    "What is the total revenue?",
    "Who are the most active users?",
    "Show all electronics products.",
    "What is the revenue by category?",
]

_TASK_DESCRIPTIONS = [
    "Analyse revenue for Q1",
    "Find top customers by spend",
    "Identify low-stock products",
    "Summarise orders from last month",
    "Compare category performance",
    "List users with most orders",
    "Calculate average order value",
    "Find best-selling product this week",
]


class ReadUser(HttpUser):
    """Simulates a read-heavy user: health checks, searches, and SQL queries.

    Weight 70 means 70% of spawned users will be this type.
    """

    weight = 70
    wait_time = between(0.5, 2)

    def on_start(self) -> None:
        logger.info("ReadUser started")

    @task(1)
    def health_check(self) -> None:
        """Poll the health endpoint."""
        self.client.get("/health", name="/health")

    @task(3)
    def search(self) -> None:
        """POST a random search query."""
        query = random.choice(_SEARCH_QUERIES)
        self.client.post(
            "/search",
            json={"query": query},
            name="/search",
        )

    @task(3)
    def sql_query(self) -> None:
        """POST a random natural-language SQL question."""
        question = random.choice(_SQL_QUESTIONS)
        self.client.post(
            "/sql/query",
            json={"question": question},
            name="/sql/query",
        )


class WriteUser(HttpUser):
    """Simulates a write user: task creation with random payloads.

    Weight 30 means 30% of spawned users will be this type.
    """

    weight = 30
    wait_time = between(1, 3)

    def on_start(self) -> None:
        logger.info("WriteUser started")

    @task
    def create_task(self) -> None:
        """POST a new task with a random description and priority."""
        self.client.post(
            "/tasks/create",
            json={
                "description": random.choice(_TASK_DESCRIPTIONS),
                "priority": random.randint(1, 5),
            },
            name="/tasks/create",
        )
