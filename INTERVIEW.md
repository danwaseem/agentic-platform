# Interview Guide — Agentic Data Platform

This document explains every component of the project in depth so you can speak confidently about design decisions, trade-offs, and implementation details.

---

## What Is This Project?

A mini production system that lets a user submit a plain-English task (e.g., "show top products by revenue") through a REST API. The system:

1. Publishes the task as an event to **Kafka**
2. An **AI agent** (LangGraph ReAct loop) consumes it, decides which tool to use, runs it, and records every step
3. Results are served back through the API, with **Redis** caching repeated queries

It demonstrates five senior-level patterns in one cohesive system: event-driven architecture, idempotency, observability, caching, and AI-powered data querying.

---

## The Five Patterns — Deep Explanation

### 1. Event-Driven Architecture with Kafka

**What it does:** Instead of calling the agent directly from the API, we publish a `TASK_CREATED` event to a Kafka topic. The agent runs as a separate consumer.

**Why Kafka?**
- **Decoupling** — the API doesn't wait for the agent to finish. It returns a `task_id` immediately.
- **Durability** — if the agent crashes mid-task, Kafka retains the event and the agent picks it up when it restarts.
- **Scalability** — you can run multiple agent instances consuming from the same topic in parallel.

**The EventEnvelope (`agent/event_types.py`):**
Every message in Kafka is wrapped in an `EventEnvelope`:
```python
class EventEnvelope(BaseModel):
    event_id: str          # UUID — unique per message
    event_type: EventType  # TASK_CREATED, AGENT_STARTED, etc.
    timestamp: datetime
    payload: dict
    idempotency_key: str   # SHA-256 hash for deduplication
    schema_version: str    # "1.0" — allows future evolution
```
This envelope pattern means every consumer knows the event type without inspecting the payload. You can add new event types without breaking existing consumers.

**Topics used:**
- `agent-tasks` — inbound tasks from the API
- `sql-queries` — SQL work requests published by the agent's ACT node
- `search-requests` — search work requests
- `agent-events` — all lifecycle events (STARTED, STEP_COMPLETED, FINISHED)

**Kafka setup:** Running in **KRaft mode** (no Zookeeper). This is the modern Kafka deployment — Zookeeper was removed in Kafka 4.0 for simplicity and better scaling.

---

### 2. Idempotency (Exactly-Once Semantics)

**The problem:** Kafka delivers messages **at-least-once**. If the agent crashes after processing but before committing the offset, it will receive the same event again when it restarts. Without a guard, the task runs twice.

**The solution (`agent/memory.py`):**
```
1. Receive event with idempotency_key = "TASK_CREATED:<sha256-of-payload>"
2. Check MongoDB dedupe collection: has this key been seen before?
   - YES → return early with "duplicate — already processed"
   - NO → process the task
3. After successful processing → insert the key into dedupe collection
```

**Why MongoDB for dedup instead of Redis?**
- MongoDB's unique index on `idempotency_key` provides a **hard uniqueness guarantee** — if two agent instances race, only one wins, the other gets a `DuplicateKeyError`.
- Redis `SET NX` would also work, but MongoDB persists across restarts while a Redis entry could expire or be flushed.

**The idempotency key (`agent/event_types.py`):**
```python
def build_idempotency_key(event_type, payload):
    payload_hash = sha256(json.dumps(payload, sort_keys=True))
    return f"{event_type.value}:{payload_hash}"
```
Keys are sorted before hashing so `{"a":1,"b":2}` and `{"b":2,"a":1}` produce the same key. This makes the key truly content-addressable.

**Interview question:** "How do you prevent double-processing?"
**Answer:** SHA-256 idempotency key derived from event type + sorted payload. Check-before-process against a MongoDB unique index. Commit the key only after the task succeeds.

---

### 3. The ReAct Agent Loop (LangGraph)

**ReAct = Reason + Act.** The agent alternates between deciding what to do (PLAN) and doing it (ACT), then observing the result, then responding.

**Why LangGraph?**
LangGraph models the agent as a `StateGraph` — a directed graph where each node is a function that transforms the agent's state. This makes the loop:
- **Auditable** — every state transition is explicit
- **Testable** — nodes are pure functions
- **Extensible** — add a new tool by adding a new node and a conditional edge

**The graph (`agent/react_agent.py`):**
```
plan → act → observe → respond → (loop back to plan OR end)
```

**Node breakdown:**

| Node | What it does |
|---|---|
| `_node_plan` | Reads the task description, pattern-matches keywords, picks a tool: `sql_query`, `search`, or `direct_response` |
| `_node_act` | Publishes a Kafka request event for the chosen tool (SQL_QUERY_REQUESTED, SEARCH_REQUESTED, etc.) |
| `_node_observe` | Calls the tool executor, gets the result, stores a trace in MongoDB |
| `_node_respond` | Formats the final answer from the observation, sets `done=True` to terminate |

**AgentState (TypedDict):**
```python
class AgentState(TypedDict):
    task_id: str
    description: str
    action: str       # chosen tool
    params: dict      # tool inputs
    observation: dict # tool output
    answer: str       # final response
    step: int         # loop counter
    done: bool        # termination signal
```
LangGraph merges each node's return dict into the existing state — nodes only return keys they modify.

**LangGraph constraint:** Every node must write at least one state key. The `_node_act` node just publishes to Kafka (a side effect) but returns `{"action": state["action"]}` to satisfy this.

**Termination:** The conditional edge after `respond` calls `_should_continue`. It returns `"end"` when `done=True` or `step >= max_steps` (default: 5).

---

### 4. Observability with MongoDB Traces

**Every agent step writes a trace document:**
```json
{
  "event_id": "uuid",
  "task_id": "t-001",
  "event_type": "AGENT_STEP_COMPLETED",
  "step": 1,
  "input_data": { "query": "top products by revenue" },
  "output_data": { "sql": "SELECT ...", "rows": [...] },
  "timestamp": "2024-03-01T10:00:00Z"
}
```

**Why MongoDB for traces?**
- Schema-flexible — observation payloads differ per tool (SQL rows vs search results)
- Native JSON storage — no mapping layer needed
- Compound indexes on `(task_id, step)` for fast retrieval

**The `GET /tasks/{task_id}` endpoint** joins the `tasks` collection (metadata) with the `traces` collection (steps), returning the full execution history. An interviewer can watch the agent's reasoning in real time.

**Indexes (`agent/memory.py`):**
```python
self._traces.create_index([("timestamp", DESCENDING)])         # latest traces
self._traces.create_index([("task_id", ASCENDING), ("step", ASCENDING)])  # task history
self._dedupe.create_index([("idempotency_key", ASCENDING)], unique=True)  # dedup guard
```

---

### 5. Redis Caching (Cache-Aside Pattern)

**Pattern:** Check cache first. On miss, compute result and write to cache. On hit, return immediately.

**Used in two places:**

**Search (`api/routers/search.py`):**
```
cache_key = sha256(query.strip().lower())
→ Redis GET → hit? return cached results
→ miss? run search, SET with TTL=3600s, return results
```

**Text-to-SQL (`api/routers/sql_query.py` + `text_to_sql/cache.py`):**
```
cache_key = sha256(question.strip().lower())
→ Redis GET → hit? return cached {sql, results}
→ miss? call Ollama → run MySQL → SET with TTL=3600s → return
```

**Performance impact:**
- Cache miss (Ollama + MySQL): ~150ms
- Cache hit (Redis): ~5ms
- **30× speedup** — visible in Locust results

**Why normalize the key?** `query.strip().lower()` means "What are the top products?" and "what are the top products?" hit the same cache entry.

---

## Text-to-SQL Deep Dive

**Flow:**
1. User posts a natural-language question to `POST /sql/query`
2. Check Redis cache — if hit, return immediately
3. Call `TextToSQLGenerator.generate()`:
   - Try Ollama: POST to `http://localhost:11434/api/generate` with the MySQL schema in the prompt
   - If Ollama is down or returns garbage: fall back to the hardcoded `_FALLBACK_MAP`
4. Validate the SQL with a regex guard — **reject anything that isn't a SELECT**
5. Execute against MySQL via SQLAlchemy
6. Cache the result in Redis, return to caller

**Safety guard (`db/mysql_client.py`):**
```python
if not sql.strip().upper().startswith("SELECT"):
    raise ValueError("Only SELECT queries are allowed")
```
This prevents prompt injection attacks where a malicious user asks the LLM to generate a `DROP TABLE`.

**The Ollama prompt:**
```
You are a SQL expert. Given the following MySQL schema, write a safe SELECT query.
Schema: {schema}
Question: {question}
Rules:
- Return only the SQL statement, nothing else.
- Only use SELECT — no INSERT, UPDATE, DELETE, DROP, or ALTER.
SQL:
```
Schema is injected at import time from `db/schema.sql` so the model knows the table structure.

**Fallback map:** Covers ~15 common demo questions with hardcoded SQL. Ensures the API works in demos even when Ollama is unavailable or slow.

---

## MySQL Schema

Three tables, 100 seed orders:

```sql
users    (id, name, email, created_at)
products (id, name, category, price, stock)
orders   (id, user_id, product_id, quantity, total, ordered_at)
```

Orders join users and products via foreign keys. This schema is enough to answer questions like:
- "Top 5 products by revenue" → GROUP BY product, SUM(total)
- "Most active user" → GROUP BY user, COUNT(orders)
- "Revenue by category" → JOIN products, GROUP BY category

---

## FastAPI Design Decisions

**Lifespan context manager:** Startup/shutdown logic lives in `@asynccontextmanager async def lifespan(app)`. This is the modern replacement for `@app.on_event("startup")`.

**Request logging middleware:** Every request logs method, path, status code, and latency in milliseconds. No external APM needed for demos.

**Pydantic models everywhere:** All request bodies and responses are typed Pydantic models. FastAPI uses these to auto-generate the Swagger UI at `/docs` and to validate inputs.

**Kafka publish failure handling:** If Kafka is unreachable, the task is still created in MongoDB with `status="kafka_unavailable"` and the API returns 201. The caller isn't left with an empty response.

---

## Testing Strategy

**All 32 tests run without any infrastructure.** No Kafka, MongoDB, or Redis needed.

**How:** `FakeProducer` and `FakeMemory` in `tests/test_agent.py` replace the real Kafka and MongoDB clients. The agent is tested purely as a state machine.

**What's tested:**
- `TestEventEnvelope` — idempotency key stability (key order doesn't matter), key uniqueness across payloads and event types
- `TestReactAgent` — returns AGENT_FINISHED, respects max_steps, publishes STARTED and FINISHED, marks event as processed, skips duplicates
- `TestDuplicateDetection` — FakeMemory before/after mark
- `TestCacheKey` (test_text_to_sql.py) — cache key determinism

---

## CI/CD

**`.github/workflows/ci.yml`** runs on every push to `main` or `dev`:

1. **Unit Tests job** — installs Python 3.11, installs requirements, runs `pytest tests/ -v`. No Docker needed because tests use in-process fakes.
2. **Docker Build job** — builds the API image from `Dockerfile`. Depends on the test job passing.

The Dockerfile uses `python:3.11-slim` to keep the image small, copies `requirements.txt` first (Docker layer caching — only reinstalls deps when requirements.txt changes), then copies the source.

---

## Common Interview Questions

**Q: Why Kafka instead of a simple HTTP call to the agent?**
A: Decoupling and durability. The API returns immediately without waiting for the agent. If the agent is down, the event is retained in Kafka and processed when it comes back up. It also enables fan-out — multiple consumers can process the same event.

**Q: How do you guarantee exactly-once processing over Kafka's at-least-once delivery?**
A: Each event carries a deterministic idempotency key (SHA-256 of event type + sorted payload). Before processing, the agent checks a MongoDB unique index for that key. If it exists, the event is skipped. After processing, the key is inserted. MongoDB's unique constraint ensures two racing agents can't both process the same event.

**Q: Why MongoDB for traces instead of a relational database?**
A: Each agent step produces a different shape of data (SQL tool returns rows, search tool returns ranked documents, direct response returns a string). A schema-free document store fits this naturally without needing separate tables or JSON columns.

**Q: How does the text-to-SQL security work?**
A: The generated SQL is regex-checked before execution — only strings starting with `SELECT` are allowed. This blocks prompt injection attacks that try to get the LLM to generate destructive SQL like `DROP TABLE`.

**Q: What's the cache key strategy and why?**
A: The question string is normalized (stripped and lowercased) then SHA-256 hashed. Normalization ensures semantically identical questions get the same cache entry. SHA-256 keeps the key short and uniform-length regardless of question length.

**Q: What would you change in production?**
A: Replace the mock search with a real vector database (Pinecone, pgvector). Add auth to the API. Use Kafka consumer groups with multiple agent replicas for horizontal scaling. Add a dead-letter topic for failed events. Use a secrets manager instead of hardcoded credentials in docker-compose. Add distributed tracing (OpenTelemetry).

**Q: What is LangGraph and why use it instead of writing the loop yourself?**
A: LangGraph models the agent loop as an explicit directed graph with typed state. This gives you conditional branching (loop vs terminate), state management, and a clear separation between nodes. Writing it yourself is possible, but you'd end up reimplementing the same graph structure without the composability benefits.

**Q: Walk me through what happens when I POST /tasks/create.**
A:
1. FastAPI validates the request body with Pydantic
2. A UUID `task_id` is generated
3. The payload is wrapped in an `EventEnvelope` with a SHA-256 idempotency key
4. The task document is immediately upserted into MongoDB `tasks` collection so `GET /tasks/{id}` works right away
5. The envelope is published to the `agent-tasks` Kafka topic
6. API returns 201 with the `task_id`
7. (Async) The ReactAgent consumes the event, runs Plan→Act→Observe→Respond, writes traces to MongoDB, publishes lifecycle events to `agent-events`

---

## Technology Choices Summary

| Decision | Alternative considered | Why this choice |
|---|---|---|
| Kafka for events | RabbitMQ, direct HTTP | Durable, replayable, at-least-once with offset control |
| LangGraph StateGraph | Custom loop, LangChain AgentExecutor | Explicit graph topology, testable pure-function nodes |
| MongoDB for traces | PostgreSQL JSONB | Schema flexibility per step type, native document model |
| Redis for caching | In-memory dict, Memcached | TTL support, survives restarts, shared across instances |
| MySQL for data | PostgreSQL, SQLite | Realistic production choice for relational e-commerce data |
| Ollama for LLM | OpenAI API | Runs fully local, no API key, no cost, demo-reliable |
| KRaft Kafka | Kafka + Zookeeper | Modern deployment, one less service to manage |
