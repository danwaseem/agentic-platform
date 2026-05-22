# Agentic Data Platform

A production-style agentic data platform built around a **ReAct (Reason + Act)** agent loop. Tasks are submitted via a REST API, published to Kafka as typed event envelopes, and processed by a stateful LangGraph agent that routes work to text-to-SQL, semantic search, or a direct response tool. Every agent step is traced to MongoDB. Redis caches repeated queries for sub-10ms reads.

---

## Architecture

```
                     ┌──────────────────────────────────────┐
                     │          FastAPI  (port 8001)         │
                     │  POST /tasks/create                   │
                     │  POST /search       ──► Redis cache   │
                     │  POST /sql/query    ──► Redis cache   │
                     │  GET  /tasks/{id}   ◄── MongoDB       │
                     └──────────────┬───────────────────────┘
                                    │ TASK_CREATED event
                                    ▼
                             ┌─────────────┐
                             │    Kafka     │  KRaft, port 9095
                             │ agent-tasks  │
                             └──────┬───────┘
                                    │ consume
                                    ▼
                         ┌──────────────────────┐
                         │      ReactAgent       │
                         │  LangGraph StateGraph │
                         │  Plan→Act→Observe     │
                         │        →Respond       │
                         └──────┬───────────┬───┘
                                │           │
              sql_query         │           │  search
                    ┌───────────┘           └───────────┐
                    ▼                                   ▼
         ┌──────────────────┐             ┌────────────────────┐
         │ TextToSQLGenerator│             │    Search Router   │
         │ Ollama (llama3)   │             │  (mock / swap in   │
         │ + fallback map    │             │   vector DB)       │
         └─────────┬────────┘             └────────────────────┘
                   │ SELECT query
                   ▼
         ┌──────────────────┐
         │    MySQL 8.0     │  port 3307
         │ users / products │
         │ orders (100 rows)│
         └──────────────────┘

  All agent steps  ──► MongoDB traces   port 27017
  Repeated queries ──► Redis cache      port 6380
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI 0.115, Uvicorn |
| Agent | ReAct loop — LangGraph `StateGraph` (`agent/react_agent.py`) |
| Event streaming | Apache Kafka 3.7 — KRaft mode (no Zookeeper) |
| Text-to-SQL | Ollama (llama3) + deterministic fallback map |
| Relational DB | MySQL 8.0 + SQLAlchemy + PyMySQL |
| Document store | MongoDB 7.0 — agent traces and task metadata |
| Cache | Redis 7.2 — SQL and search results, TTL = 1 hour |
| Load testing | Locust 2.28 |
| Tests | pytest + pytest-asyncio (32 unit tests, no infrastructure required) |
| CI/CD | GitHub Actions — test + docker-build on push to `main` / `dev` |

---

## Prerequisites

- Docker Desktop
- Python 3.11+
- `mysql` CLI (for seeding)

**Port mapping**

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
# 1. Start all infrastructure
docker compose up -d

# 2. Pull the LLM model into Ollama (one-time, ~4 GB)
docker exec agentdb_ollama ollama pull llama3

# 3. Create and activate a Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt

# 5. Seed MySQL with schema and demo data
mysql -h 127.0.0.1 -P 3307 -u danish -pdanish123 agentdb < db/schema.sql

# 6. Start the API server
uvicorn api.main:app --reload --port 8001
```

---

## API

```bash
# Health check
curl http://localhost:8001/health

# Create a task — publishes TASK_CREATED to Kafka
curl -X POST http://localhost:8001/tasks/create \
  -H 'Content-Type: application/json' \
  -d '{"description": "Find top products by revenue", "priority": 3}'

# Retrieve task and all agent trace steps
curl http://localhost:8001/tasks/<task_id>

# Semantic search (Redis-cached after first call)
curl -X POST http://localhost:8001/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "revenue trends"}'

# Text-to-SQL (Ollama or fallback, Redis-cached)
curl -X POST http://localhost:8001/sql/query \
  -H 'Content-Type: application/json' \
  -d '{"question": "What are the top 5 products by revenue?"}'

# Interactive docs
open http://localhost:8001/docs
```

---

## Run the Agent Smoke Test

```bash
# Runs three tasks through the full Plan→Act→Observe→Respond loop
# and verifies the idempotency guard blocks duplicate events
python smoke_agent.py
```

---

## Tests

```bash
pytest tests/ -v
```

All 32 tests run without any infrastructure (Kafka, MongoDB, Redis are fully mocked).

---

## Load Testing

```bash
# 50 users, 60-second headless run (70% read / 30% write)
locust -f locustfile.py \
  --host=http://localhost:8001 \
  --users 50 \
  --spawn-rate 5 \
  --run-time 60s \
  --headless

# Web UI at http://localhost:8089
locust -f locustfile.py --host=http://localhost:8001
```

---

## Project Structure

```
agentic-platform/
├── agent/
│   ├── event_types.py       # EventEnvelope, EventType enum, idempotency key builder
│   ├── kafka_consumer.py    # Kafka consumer wrapper
│   ├── kafka_producer.py    # Kafka producer wrapper
│   ├── memory.py            # MongoDB-backed trace storage and dedup
│   └── react_agent.py       # LangGraph StateGraph ReAct loop
├── api/
│   ├── main.py              # FastAPI app, middleware, lifespan
│   └── routers/
│       ├── search.py        # POST /search with Redis cache
│       ├── sql_query.py     # POST /sql/query with Redis cache
│       └── tasks.py         # POST /tasks/create, GET /tasks/{id}
├── db/
│   ├── mongo_client.py      # MongoDB singleton client
│   ├── mysql_client.py      # MySQL connection and safe query executor
│   ├── redis_client.py      # Redis singleton client
│   └── schema.sql           # DDL + 100-row seed (users, products, orders)
├── text_to_sql/
│   ├── cache.py             # Redis cache wrapper for SQL results
│   └── generator.py         # Ollama client + deterministic fallback map
├── tests/
│   ├── test_agent.py        # EventEnvelope, ReactAgent, dedup tests
│   └── test_text_to_sql.py  # Cache key stability and generator tests
├── docker-compose.yml       # MySQL, MongoDB, Redis, Kafka, Ollama
├── Dockerfile               # API service image (python:3.11-slim)
├── locustfile.py            # Load test scenarios
├── smoke_agent.py           # End-to-end agent smoke test
└── requirements.txt
```
