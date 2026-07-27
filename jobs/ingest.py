# add it to the clickhouse trace db
import logging
import time
import uuid
from typing import Any

from db.clickhouse import clickhouse_client
from db.kafka import kafka_producer
from jobs.helper.cost_estimator import estimate_trace_cost, normalize_model_name
from jobs.helper.dataset_autoappend import maybe_autoappend
from jobs.helper.root_resolver import root_resolver

logger = logging.getLogger(__name__)

# Stable namespace for mapping non-UUID identifiers to deterministic UUIDs. The
# API boundary already normalizes these, but a defensive coercion here keeps a
# malformed id from any producer from crashing the consumer on the UUID column
# (ClickHouse stores trace_id/root_trace_id as UUID) and stalling the partition.
_TRACE_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://getfluiq.com/trace-id")


def _coerce_uuid(value: Any) -> Any:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return str(uuid.UUID(s))
    except (ValueError, AttributeError, TypeError):
        return str(uuid.uuid5(_TRACE_NS, s))


def _agent_identity(event: dict[str, Any]) -> tuple[str, str]:
    """Derive (agent_key, agent_kind) from an event, mirroring the Agents query.

    Precedence matches the old read-side multiIf(): an explicit @trace
    ``function`` name, else a chain ``name``, else a LangGraph node. Returns
    ("", "") for spans that carry none of these (e.g. bare LLM calls), which the
    Agents view filters out with ``agent_key != ''``.
    """
    fn = str(event.get("function") or "")
    if fn:
        return fn, "function"
    nm = str(event.get("name") or "")
    if nm:
        return nm, "chain"
    langgraph = event.get("langgraph")
    lg = str(langgraph.get("langgraph_node") or "") if isinstance(langgraph, dict) else ""
    if lg:
        return lg, "langgraph_node"
    return "", ""


async def ingest_trace(message: dict[str, Any]) -> None:
    """Operation 1: persist the trace event, then estimate and store its cost.

    The trace is written to ``fluiq.traces`` first so that the cost row in
    ``fluiq.trace_costs`` always references a trace that exists. A ``trace_id``
    is generated when the SDK did not provide one and is propagated into the
    event payload so it survives in the JSON column as well. The
    ``root_trace_id`` is resolved from the parent chain so per-agent rollups
    on the read side become a simple ``GROUP BY``.

    ``status="running"`` events are live-progress signals emitted by the SDK
    before the wrapped call completes. They share their trace_id with the
    eventual completion event; persisting them would double-count rows and
    pollute cost/agent rollups, so we publish them to the SSE broker only.
    """
    event = message.get("event") or {}
    trace_id = _coerce_uuid(event.get("trace_id") if isinstance(event, dict) else None) or str(uuid.uuid4())
    parent_id = _coerce_uuid(event.get("parent_id") if isinstance(event, dict) else None)
    organization_id = message.get("organization_id")
    status = event.get("status") if isinstance(event, dict) else None
    is_running = status == "running"

    root_trace_id, is_root = await root_resolver.resolve_with_flag(
        trace_id=trace_id,
        parent_id=parent_id,
        organization_id=organization_id,
    )
    # A span that is its own root is always a root — the fundamental definition,
    # matching the old read-time ``trace_id = root_trace_id`` check. This guards
    # against a self-referential or stale parent_id fooling the parent-based
    # resolver (e.g. CrewAI's crew span emits parent_id == its own trace_id,
    # which otherwise stamps is_root=0 and hides the run from the roots list).
    if root_trace_id == trace_id:
        is_root = True

    # Denormalize the agent-run identity onto the row so the read side can
    # filter/group on plain columns instead of re-extracting these JSON paths
    # (and running a whole-org NOT IN) on every dashboard load. Mirrors the
    # multiIf() the Agents query used: function > name > langgraph_node.
    agent_key, agent_kind = _agent_identity(event if isinstance(event, dict) else {})
    message["is_root"] = 1 if is_root else 0
    message["agent_key"] = agent_key
    message["agent_kind"] = agent_kind

    if isinstance(event, dict):
        event["trace_id"] = trace_id
        event["root_trace_id"] = root_trace_id
        # Normalise Vertex AI resource-path model names (e.g.
        # "publishers/google/models/gemini-2.5-flash") to the bare id so the
        # stored event reads cleanly in the UI and the cost lookup matches.
        if event.get("model"):
            event["model"] = normalize_model_name(event["model"])
        message["event"] = event
    message["trace_id"] = trace_id
    message["root_trace_id"] = root_trace_id

    # Cache the parent-chain mapping unconditionally so descendant start /
    # completion events can resolve their root before this trace's own
    # completion lands.
    root_resolver.remember(trace_id, root_trace_id)

    if is_running:
        started = {
            "kind": "started",
            "organization_id": str(organization_id),
            "api_key_prefix": message.get("api_key_prefix"),
            "trace_id": trace_id,
            "root_trace_id": root_trace_id,
            "ingested_at_ms": int(time.time() * 1000),
            "event": event if isinstance(event, dict) else {},
        }
        try:
            await kafka_producer.publish(started, key=str(organization_id))
        except Exception:
            logger.exception(
                "[TRACER] Failed to publish traces.started trace_id=%s", trace_id,
            )
        return

    await clickhouse_client.insert_trace(message)
    logger.info(
        "[TRACER] Ingested trace org=%s prefix=%s trace_id=%s root=%s",
        organization_id,
        message.get("api_key_prefix"),
        trace_id,
        root_trace_id,
    )

    # Notify the API's in-process SSE broker that this trace is now durable
    # and queryable. Payload mirrors the TraceRecord shape consumed by the
    # /traces endpoint so the frontend can prepend without a re-fetch. The
    # ``kind`` discriminator lets the SSE route emit different event names
    # ("trace" vs "trace.enriched") for the same Kafka topic.
    persisted = {
        "kind": "persisted",
        "organization_id": str(organization_id),
        "api_key_prefix": message.get("api_key_prefix"),
        "trace_id": trace_id,
        "root_trace_id": root_trace_id,
        "ingested_at_ms": int(time.time() * 1000),
        "event": event if isinstance(event, dict) else {},
    }
    try:
        await kafka_producer.publish(persisted, key=str(organization_id))
    except Exception:
        logger.exception(
            "[TRACER] Failed to publish traces.persisted trace_id=%s", trace_id,
        )

    # Auto-append this run to any dataset its agent is linked to (Connect Agents).
    # Root traces only — the agent identity is derived from the root event, and
    # is best-effort so a hiccup never blocks ingest.
    if trace_id == root_trace_id:
        await maybe_autoappend(event if isinstance(event, dict) else {}, organization_id, trace_id)

    try:
        breakdown = await estimate_trace_cost(event)
    except Exception:
        logger.exception("[COST] estimate_trace_cost failed trace_id=%s", trace_id)
        return

    if breakdown is None:
        return

    cost_record = {
        "organization_id": organization_id,
        "api_key_prefix": message.get("api_key_prefix"),
        "trace_id": trace_id,
        "root_trace_id": root_trace_id,
        **breakdown,
    }
    try:
        await clickhouse_client.insert_cost(cost_record)
    except Exception:
        logger.exception("[COST] insert_cost failed trace_id=%s", trace_id)
        return

    logger.info(
        "[COST] trace_id=%s provider=%s model=%s total=%s",
        trace_id,
        breakdown.get("provider"),
        breakdown.get("model"),
        breakdown.get("total_cost"),
    )

    # Fan out the cost enrichment so connected SSE clients can merge it
    # into the row they already have without a re-fetch. ``total_cost`` is
    # a Decimal coming out of the price math; cast to float so the JSON
    # serializer doesn't fall back to ``str``.
    enriched_cost = {
        "kind": "enriched",
        "enrichment": "cost",
        "organization_id": str(organization_id),
        "api_key_prefix": message.get("api_key_prefix"),
        "trace_id": trace_id,
        "root_trace_id": root_trace_id,
        "cost": float(breakdown.get("total_cost") or 0.0),
        "currency": breakdown.get("currency") or "USD",
    }
    try:
        await kafka_producer.publish(enriched_cost, key=str(organization_id))
    except Exception:
        logger.exception(
            "[TRACER] Failed to publish trace.enriched (cost) trace_id=%s", trace_id,
        )