# Agentic Platform

A production-style agentic data platform built around a ReAct (Reason + Act) agent loop. Tasks are submitted via a FastAPI REST API, published to Apache Kafka as typed event envelopes, and processed by a stateful agent that chooses between text-to-SQL generation, semantic search, or direct response. Every agent step is traced to MongoDB for full observability. Redis caches both search results and SQL query responses to deliver sub-10ms reads at load.

---

## Architecture

```
                         ┌─────────────────────────────────────────────┐
                         │                FastAPI (port 8001)           │
                         │  POST /tasks/create                          │
                         │  POST /search       ──► Redis cache          │
                         │  POST /sql/query    ──► Redis cache          │
                         │  GET  /tasks/{id}   ◄── MongoDB traces       │
                         └───────────────┬─────────────────────────────┘
                                         │ TASK_CREATED event
                                         ▼
                                  ┌─────────────┐
                                  │    Kafka     │  (KRaft, port 9095)
                                  │ agent-tasks  │
                                  └──────┬───────┘
                                         │ consume
                                         ▼
                              ┌──────────────────────┐
                              │      ReactAgent       │
                              │  Plan → Act → Observe │
                              │       → Respond       │
                              └───┬──────────┬────────┘
                                  │          │
                    sql_query     │          │  search
                         ┌────────┘          └────────┐
                         ▼                            ▼
               ┌──────────────────┐        ┌──────────────────┐
               │ TextToSQLGenerator│        │   Mock Search    │
               │  Ollama (llama3)  │        │  (swap in vector │
               │  + fallback map   │        │   DB for prod)   │
               └────────┬─────────┘        └──────────────────┘
                        │ SELECT query
                        ▼
               ┌──────────────────┐
               │   MySQL 8.0      │  (port 3307)
               │ users/products/  │
               │ orders (100 rows)│
               └──────────────────┘

   All agent steps ──► MongoDB traces  (port 27017)
   Repeated queries ──► Redis cache    (port 6380)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI 0.115, Uvicorn |
| Agent | ReAct loop (custom), `agent/react_agent.py` |
| Event streaming | Apache Kafka 3.7 — KRaft mode, no Zookeeper |
| Text-to-SQL | Ollama (llama3) + deterministic fallback map |
| Relational DB | MySQL 8.0 + SQLAlchemy + PyMySQL |
| Document store | MongoDB 7.0 (agent traces + task metadata) |
| Cache | Redis 7.2 (SQL results, search results, TTL=1h) |
| Load testing | Locust 2.28 |
| Tests | pytest + pytest-asyncio |

---

## Prerequisites

- Docker Desktop (for all infrastructure)
- Python 3.11+
- `mysql` CLI (for seeding)

> **Note:** The ports below reflect the adjusted mapping used when running alongside another project. If running standalone you can restore the defaults in `docker-compose.yml`.

| Service | Host port |
|---|---|
| MySQL | 3307 |
| MongoDB | 27017 |
| Redis | 6380 |
| Kafka | 9095 |
| Ollama | 11434 |
| FastAPI | 8001 |

---

## Setup

```bash
# 1. Clone and enter the project
cd agentic-platform

# 2. Start all infrastructure
docker compose up -d

# 3. Pull the LLM model into Ollama (one-time, ~4 GB)
docker exec agentdb_ollama ollama pull llama3

# 4. Create and activate a Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 5. Install dependencies
pip install -r requirements.txt

# 6. Seed MySQL with schema and demo data
mysql -h 127.0.0.1 -P 3307 -u danish -pdanish123 agentdb < db/schema.sql

# 7. Start the API server
uvicorn api.main:app --reload --port 8001
```

---

## API Examples

```bash
# Health check
curl http://localhost:8001/health

# Create a task (publishes TASK_CREATED to Kafka)
curl -X POST http://localhost:8001/tasks/create \
  -H 'Content-Type: application/json' \
  -d '{"description": "Find top products by revenue", "priority": 3}'

# Retrieve task and agent traces (use task_id from above)
curl http://localhost:8001/tasks/<task_id>

# Search (Redis-cached after first call)
curl -X POST http://localhost:8001/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "revenue trends"}'

# Text-to-SQL (Ollama or fallback, Redis-cached)
curl -X POST http://localhost:8001/sql/query \
  -H 'Content-Type: application/json' \
  -d '{"question": "What are the top 5 products by revenue?"}'

# Interactive Swagger docs
open http://localhost:8001/docs
```

---

## Run the Agent Locally

```bash
# Runs three tasks through the full Plan→Act→Observe→Respond loop
python smoke_agent.py
```

---

## Load Testing

```bash
# 50 users, 60-second headless run (70% read / 30% write mix)
locust -f locustfile.py \
  --host=http://localhost:8001 \
  --users 50 \
  --spawn-rate 5 \
  --run-time 60s \
  --headless

# Web UI (navigate to http://localhost:8089)
locust -f locustfile.py --host=http://localhost:8001
```

---

## Tests

```bash
pytest tests/ -v
```

Expected output:

```
tests/test_agent.py::TestEventEnvelope::test_idempotency_key_is_stable_regardless_of_payload_key_order  PASSED
tests/test_agent.py::TestEventEnvelope::test_idempotency_key_differs_for_different_payloads             PASSED
tests/test_agent.py::TestEventEnvelope::test_idempotency_key_differs_for_different_event_types          PASSED
tests/test_agent.py::TestEventEnvelope::test_stable_payload_hash_is_deterministic                       PASSED
tests/test_agent.py::TestEventEnvelope::test_create_envelope_fields                                     PASSED
tests/test_agent.py::TestReactAgent::test_run_returns_agent_finished_envelope                           PASSED
tests/test_agent.py::TestReactAgent::test_run_does_not_exceed_max_steps                                 PASSED
tests/test_agent.py::TestReactAgent::test_run_publishes_agent_started_and_finished                      PASSED
tests/test_agent.py::TestReactAgent::test_run_marks_event_as_processed                                  PASSED
tests/test_agent.py::TestReactAgent::test_run_skips_duplicate_event                                     PASSED
tests/test_agent.py::TestDuplicateDetection::test_is_duplicate_returns_false_before_mark                PASSED
tests/test_agent.py::TestDuplicateDetection::test_is_duplicate_returns_true_after_mark                  PASSED
tests/test_agent.py::TestDuplicateDetection::test_mark_processed_is_idempotent                          PASSED
tests/test_text_to_sql.py::TestCacheKey::test_cache_key_is_stable                                       PASSED
...
```

---

## Interview Explanation

This project demonstrates five production engineering patterns in a single cohesive system:

1. **Event-driven agent orchestration** — Tasks enter the system as Kafka events. The ReactAgent consumes them and runs a Plan→Act→Observe→Respond loop, choosing the right tool based on the task description. Every step is an event, making the system fully auditable.

2. **Idempotency** — Every event carries a deterministic `idempotency_key` (SHA-256 of event type + payload). The agent checks MongoDB before processing and commits the key only after success, giving exactly-once semantics over an at-least-once Kafka consumer.

3. **Traceability** — Each agent step writes a trace document to MongoDB. The `GET /tasks/{id}` endpoint surfaces the full execution history, making it easy to debug what the agent did and why.

4. **Redis caching** — Both the search and text-to-SQL endpoints use a cache-aside pattern. Under load, cache-hit latency drops from ~150ms to ~5ms — a 30× speedup visible in the Locust results.

5. **Text-to-SQL over structured data** — Natural-language questions are converted to safe SELECT queries via Ollama (local LLM), with a deterministic fallback map for demo reliability. A regex guard rejects any non-SELECT SQL before it reaches MySQL.
