"""Auto-append linked-agent runs into datasets.

When a user connects an agent to a dataset (Datasets → Connect Agents), a row is
written to ``dataset_agent_links``. This hook fires on every completed *root*
trace: if the trace's agent is linked to any dataset, the run is appended to
those datasets as an example — the "future runs auto-append" half of Connect
Agents (past runs are imported synchronously by the API at link time).

Everything here is best-effort: a failure never affects trace ingestion. A tiny
per-org TTL cache of linked (agent_key, agent_kind) pairs keeps the common case
(no links) down to one cheap query per org per minute instead of a lookup on
every trace.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

from db.postgresql import postgres_client

logger = logging.getLogger(__name__)

# org_id -> (expiry_epoch, set[(agent_key, agent_kind)])
_LINK_CACHE: dict[str, tuple[float, set[tuple[str, str]]]] = {}
_CACHE_TTL = 60.0


def derive_agent(event: dict) -> tuple[Optional[str], Optional[str]]:
    """Agent (key, kind) for a root event — mirrors ``fetch_agent_summary``'s
    ``multiIf(function -> 'function', name -> 'chain', langgraph_node ...)``.
    Returns (None, None) for un-named roots (raw LLM calls), which aren't agents.
    """
    fn = event.get("function") or ""
    if isinstance(fn, str) and fn:
        return fn, "function"
    nm = event.get("name") or ""
    if isinstance(nm, str) and nm:
        return nm, "chain"
    lg = event.get("langgraph")
    node = lg.get("langgraph_node") if isinstance(lg, dict) else None
    if isinstance(node, str) and node:
        return node, "langgraph_node"
    return None, None


def _event_to_example(event: dict) -> tuple[str, Optional[str], dict]:
    """Build (input, expected_output, metadata) — mirror of the API-side helper."""
    req = (
        event.get("messages")
        or event.get("input")
        or event.get("contents")
        or event.get("prompts")
    )
    if isinstance(req, str):
        input_text = req
    elif req is not None:
        input_text = json.dumps(req, default=str)
    else:
        input_text = ""

    resp = event.get("output")
    if resp is None:
        resp = event.get("response")
    if resp is None:
        expected: Optional[str] = None
    elif isinstance(resp, str):
        expected = resp
    else:
        expected = json.dumps(resp, default=str)

    metadata: dict[str, Any] = {}
    model = event.get("model")
    if isinstance(model, str) and model:
        metadata["model"] = model
    tid = event.get("trace_id")
    if isinstance(tid, str) and tid:
        metadata["source_trace_id"] = tid
    integ = event.get("integration")
    if isinstance(integ, str) and integ:
        metadata["integration"] = integ
    return input_text, expected, metadata


async def _linked_pairs(org_id: str) -> set[tuple[str, str]]:
    """Linked (agent_key, agent_kind) pairs for an org, cached for _CACHE_TTL."""
    now = time.monotonic()
    cached = _LINK_CACHE.get(org_id)
    if cached and cached[0] > now:
        return cached[1]
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT agent_key, agent_kind FROM dataset_agent_links WHERE org_id = $1",
            org_id,
        )
    pairs = {(r["agent_key"], r["agent_kind"]) for r in rows}
    _LINK_CACHE[org_id] = (now + _CACHE_TTL, pairs)
    return pairs


async def maybe_autoappend(event: dict, organization_id: Any, trace_id: str) -> None:
    """Append this root run to any dataset its agent is linked to (best-effort)."""
    if not isinstance(event, dict) or not organization_id:
        return
    org_id = str(organization_id)
    try:
        agent_key, agent_kind = derive_agent(event)
        if not agent_key:
            return
        pairs = await _linked_pairs(org_id)
        if (agent_key, agent_kind) not in pairs:
            return

        input_text, expected, metadata = _event_to_example(event)
        # Capture the run if it has ANY representable content. Agentic wrapper
        # roots (e.g. CrewAI's crew span) carry no direct input but do carry the
        # run's final output — and metadata.source_trace_id preserves the whole
        # trajectory, which a dataset run re-reads for eval. Skipping on empty
        # input alone would drop every CrewAI run.
        if not input_text.strip() and not (expected and str(expected).strip()):
            return

        async with postgres_client.acquire() as conn:
            dataset_rows = await conn.fetch(
                "SELECT dataset_id FROM dataset_agent_links "
                "WHERE org_id = $1 AND agent_key = $2 AND agent_kind = $3",
                org_id, agent_key, agent_kind,
            )
            for dr in dataset_rows:
                dataset_id = dr["dataset_id"]
                # Dedupe: skip if this run is already an example in the dataset.
                exists = await conn.fetchrow(
                    "SELECT 1 FROM dataset_examples "
                    "WHERE dataset_id = $1 AND org_id = $2 "
                    "AND metadata->>'source_trace_id' = $3",
                    dataset_id, org_id, trace_id,
                )
                if exists is not None:
                    continue
                await conn.execute(
                    "INSERT INTO dataset_examples "
                    "(dataset_id, org_id, input, expected_output, metadata) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb)",
                    dataset_id, org_id, input_text, expected, json.dumps(metadata),
                )
                logger.info(
                    "[TRACER] auto-appended trace_id=%s to dataset=%s (agent=%s)",
                    trace_id, dataset_id, agent_key,
                )
    except Exception:
        logger.exception("[TRACER] dataset auto-append failed trace_id=%s", trace_id)
