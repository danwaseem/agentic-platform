"""LangGraph-style ReAct agent: Plan → Act → Observe → Respond.

The agent state flows through a compiled StateGraph. Each node is a pure
function that receives the current AgentState and returns a partial update.
Conditional edges decide whether to loop or terminate.
"""

import logging
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from agent.event_types import EventEnvelope, EventType, create_envelope
from agent.kafka_producer import KafkaEventProducer
from agent.memory import AgentMemory

logger = logging.getLogger(__name__)

_SQL_KEYWORDS = {"sql", "database", "db", "orders", "products", "revenue", "table", "query", "rows"}
_SEARCH_KEYWORDS = {"search", "find", "look up", "lookup", "fetch", "retrieve"}


# ------------------------------------------------------------------ #
# Graph state
# ------------------------------------------------------------------ #

class AgentState(TypedDict):
    """Immutable snapshot passed between LangGraph nodes.

    Each node returns a dict with only the keys it updates; LangGraph merges
    it into the previous state automatically.
    """
    task_id: str
    description: str
    action: str                    # chosen by plan node
    params: dict[str, Any]         # inputs for the chosen tool
    observation: dict[str, Any]    # result from act/observe node
    answer: str                    # built by respond node
    step: int                      # incremented each loop iteration
    done: bool                     # set True by respond node to terminate


# ------------------------------------------------------------------ #
# Mock tool executor
# ------------------------------------------------------------------ #

def mock_tool_executor(action: str, params: dict[str, Any]) -> dict[str, Any]:
    """Return a canned result for a given action, simulating a downstream worker.

    In production each action would call a real service (MySQL, vector DB, etc.).

    Args:
        action: One of 'sql_query', 'search', or 'direct_response'.
        params: Inputs for the tool.

    Returns:
        Dict representing the tool's output.
    """
    if action == "sql_query":
        return {
            "status": "ok",
            "sql": f"SELECT * FROM orders WHERE description LIKE '%{params.get('query', '')}%'",
            "rows": [
                {"order_id": 1, "product": "Widget A", "revenue": 299.99},
                {"order_id": 2, "product": "Widget B", "revenue": 149.50},
            ],
            "row_count": 2,
        }
    if action == "search":
        return {
            "status": "ok",
            "results": [
                {"title": f"Result for: {params.get('query', '')}", "score": 0.95},
                {"title": "Related document", "score": 0.80},
            ],
            "result_count": 2,
        }
    return {
        "status": "ok",
        "answer": f"Handled directly: {params.get('description', '')}",
    }


# ------------------------------------------------------------------ #
# ReactAgent
# ------------------------------------------------------------------ #

class ReactAgent:
    """Event-driven agent built on a LangGraph StateGraph.

    The graph has four nodes — plan, act, observe, respond — connected by
    edges that mirror the classic ReAct loop. A conditional edge after
    'respond' either loops back to 'plan' for another step or terminates
    at END when max_steps is reached or the agent signals done=True.

    Each task is processed idempotently: duplicate events are detected via
    AgentMemory before the graph is invoked.

    Args:
        producer: KafkaEventProducer used to publish all lifecycle events.
        memory: AgentMemory for trace storage and deduplication.
        task_topic: Inbound task topic.
        sql_topic: SQL query request topic.
        search_topic: Search request topic.
        agent_topic: Agent lifecycle event topic.
        max_steps: Hard ceiling on graph loop iterations.
    """

    def __init__(
        self,
        producer: KafkaEventProducer,
        memory: AgentMemory,
        task_topic: str = "agent-tasks",
        sql_topic: str = "sql-queries",
        search_topic: str = "search-requests",
        agent_topic: str = "agent-events",
        max_steps: int = 5,
    ) -> None:
        self.producer = producer
        self.memory = memory
        self.task_topic = task_topic
        self.sql_topic = sql_topic
        self.search_topic = search_topic
        self.agent_topic = agent_topic
        self.max_steps = max_steps
        self._graph = self._build_graph()

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build_graph(self):
        """Compile the LangGraph StateGraph for the ReAct loop.

        Graph topology:
            plan → act → observe → respond → (loop back to plan OR END)
        """
        g = StateGraph(AgentState)

        g.add_node("plan", self._node_plan)
        g.add_node("act", self._node_act)
        g.add_node("observe", self._node_observe)
        g.add_node("respond", self._node_respond)

        g.set_entry_point("plan")
        g.add_edge("plan", "act")
        g.add_edge("act", "observe")
        g.add_edge("observe", "respond")

        # Conditional edge: loop or terminate
        g.add_conditional_edges(
            "respond",
            self._should_continue,
            {"continue": "plan", "end": END},
        )

        return g.compile()

    def _should_continue(self, state: AgentState) -> str:
        """Return 'end' when the agent signals done or max_steps is reached."""
        if state["done"] or state["step"] >= self.max_steps:
            return "end"
        return "continue"

    # ------------------------------------------------------------------
    # Graph nodes
    # ------------------------------------------------------------------

    def _node_plan(self, state: AgentState) -> dict[str, Any]:
        """PLAN: choose which tool to invoke based on the task description."""
        lower = state["description"].lower()

        if any(kw in lower for kw in _SQL_KEYWORDS):
            action = "sql_query"
            params: dict[str, Any] = {
                "query": state["description"],
                "task_description": state["description"],
            }
        elif any(kw in lower for kw in _SEARCH_KEYWORDS):
            action = "search"
            params = {"query": state["description"]}
        else:
            action = "direct_response"
            params = {"description": state["description"]}

        logger.info("PLAN  task_id=%s action=%s", state["task_id"], action)
        return {"action": action, "params": params}

    def _node_act(self, state: AgentState) -> dict[str, Any]:
        """ACT: publish the appropriate Kafka request event for the chosen action."""
        payload = {"task_id": state["task_id"], **state["params"]}

        if state["action"] == "sql_query":
            event_type, topic = EventType.SQL_QUERY_REQUESTED, self.sql_topic
        elif state["action"] == "search":
            event_type, topic = EventType.SEARCH_REQUESTED, self.search_topic
        else:
            event_type, topic = EventType.AGENT_STEP_COMPLETED, self.agent_topic

        envelope = create_envelope(event_type, payload)
        self.producer.publish(topic, envelope)
        logger.info("ACT   task_id=%s published %s", state["task_id"], event_type.value)
        return {"action": state["action"]}  # LangGraph requires at least one key written

    def _node_observe(self, state: AgentState) -> dict[str, Any]:
        """OBSERVE: run the mock tool and store the trace."""
        observation = mock_tool_executor(state["action"], state["params"])
        logger.info("OBSERVE task_id=%s %s", state["task_id"], observation)

        step_env = create_envelope(
            EventType.AGENT_STEP_COMPLETED,
            {
                "task_id": state["task_id"],
                "step": state["step"] + 1,
                "action": state["action"],
                "observation": observation,
            },
        )
        self.producer.publish(self.agent_topic, step_env)
        self._store_trace(
            step_env,
            state["task_id"],
            step=state["step"] + 1,
            input_data=state["params"],
            output_data=observation,
        )
        return {"observation": observation, "step": state["step"] + 1}

    def _node_respond(self, state: AgentState) -> dict[str, Any]:
        """RESPOND: build the final answer and signal the graph to terminate."""
        obs = state["observation"]
        action = state["action"]

        if action == "sql_query":
            rows = obs.get("rows", [])
            answer = (
                f"SQL query returned {len(rows)} row(s). "
                f"Generated SQL: {obs.get('sql', 'n/a')}"
            )
        elif action == "search":
            results = obs.get("results", [])
            top = results[0]["title"] if results else "no results"
            answer = f"Search returned {len(results)} result(s). Top result: {top}"
        else:
            answer = obs.get("answer", f"Completed: {state['description']}")

        logger.info("RESPOND task_id=%s answer=%r", state["task_id"], answer)
        # Single-step by default — set done=True to stop after first tool call.
        # Remove this to allow multi-step reasoning loops.
        return {"answer": answer, "done": True}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, task_event: EventEnvelope) -> EventEnvelope:
        """Invoke the StateGraph for a single task event.

        Args:
            task_event: EventEnvelope with 'task_id' and 'description' in payload.

        Returns:
            AGENT_FINISHED EventEnvelope containing the final answer.
        """
        task_id: str = task_event.payload.get("task_id", task_event.event_id)
        description: str = task_event.payload.get("description", "")

        # --- Idempotency guard ------------------------------------------
        if self.memory.is_duplicate(task_event.idempotency_key):
            logger.warning("Duplicate event — skipping task_id=%s", task_id)
            return create_envelope(
                EventType.AGENT_FINISHED,
                {"task_id": task_id, "answer": "duplicate — already processed", "steps": 0},
            )

        logger.info("ReactAgent starting task_id=%s", task_id)

        # --- Publish AGENT_STARTED --------------------------------------
        started_env = create_envelope(
            EventType.AGENT_STARTED, {"task_id": task_id, "description": description}
        )
        self.producer.publish(self.agent_topic, started_env)
        self._store_trace(
            started_env, task_id, step=0,
            input_data=task_event.payload, output_data={}
        )

        # --- Run the LangGraph StateGraph --------------------------------
        initial_state: AgentState = {
            "task_id": task_id,
            "description": description,
            "action": "",
            "params": {},
            "observation": {},
            "answer": "",
            "step": 0,
            "done": False,
        }
        final_state: AgentState = self._graph.invoke(initial_state)

        # --- Publish AGENT_FINISHED -------------------------------------
        finished_env = create_envelope(
            EventType.AGENT_FINISHED,
            {
                "task_id": task_id,
                "answer": final_state["answer"],
                "steps": final_state["step"],
            },
        )
        self.producer.publish(self.agent_topic, finished_env)
        self._store_trace(
            finished_env, task_id,
            step=final_state["step"] + 1,
            input_data={},
            output_data={"answer": final_state["answer"]},
        )

        self.memory.mark_processed(task_event.idempotency_key, task_event.event_id)
        logger.info("ReactAgent finished task_id=%s answer=%r", task_id, final_state["answer"])
        return finished_env

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _store_trace(
        self,
        envelope: EventEnvelope,
        task_id: str,
        step: int,
        input_data: dict[str, Any],
        output_data: dict[str, Any],
    ) -> None:
        self.memory.store_trace(
            event_id=envelope.event_id,
            task_id=task_id,
            event_type=envelope.event_type.value,
            step=step,
            input_data=input_data,
            output_data=output_data,
            timestamp=datetime.now(tz=timezone.utc),
        )
