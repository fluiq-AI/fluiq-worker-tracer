"""Resolve the root_trace_id for an incoming trace.

A "root" is the trace_id of the outermost @trace-decorated function (or
LangChain chain root, or LangGraph entry) of which the current trace is a
descendant. Leaf LLM calls without any wrapping decorator inherit a
synthetic ``chain_id`` from the SDK's prompt-prefix detector — that
synthetic id is treated as the root for those calls, which means siblings
sharing the same chain bucket together.

Resolution strategy:
  1. No parent_id   -> this trace IS its own root.
  2. parent_id seen -> consult the in-process LRU cache (fast path).
  3. cache miss     -> query ClickHouse for the parent's root_trace_id.
  4. nothing found  -> fall back to using parent_id itself as the root.
                       This handles synthetic chain_ids (no traces row) and
                       out-of-order Kafka delivery where the parent has
                       not yet been persisted.
"""
from __future__ import annotations

import logging
import uuid
from collections import OrderedDict
from typing import Optional

from db.clickhouse import clickhouse_client

logger = logging.getLogger(__name__)

_CACHE_MAX = 10_000


class RootTraceResolver:
    def __init__(self, max_size: int = _CACHE_MAX) -> None:
        self._cache: "OrderedDict[str, str]" = OrderedDict()
        self._max = max_size

    def _put(self, trace_id: str, root_trace_id: str) -> None:
        if trace_id in self._cache:
            self._cache.move_to_end(trace_id)
            self._cache[trace_id] = root_trace_id
            return
        self._cache[trace_id] = root_trace_id
        if len(self._cache) > self._max:
            self._cache.popitem(last=False)

    def _get(self, trace_id: str) -> Optional[str]:
        v = self._cache.get(trace_id)
        if v is not None:
            self._cache.move_to_end(trace_id)
        return v

    def remember(self, trace_id: str, root_trace_id: str) -> None:
        """Stash a (trace_id, root_trace_id) pair after a successful insert."""
        self._put(trace_id, root_trace_id)

    async def resolve(
        self,
        trace_id: str,
        parent_id: Optional[str],
        organization_id: Optional[str] = None,
    ) -> str:
        if not parent_id:
            return trace_id

        cached = self._get(parent_id)
        if cached:
            return cached

        if not _looks_like_uuid(parent_id):
            # Non-UUID parent (shouldn't normally happen) — treat as own root.
            return parent_id

        try:
            root = await _query_parent_root(parent_id, organization_id)
        except Exception:
            logger.exception("[ROOT] ClickHouse lookup failed parent_id=%s", parent_id)
            root = None

        if root:
            self._put(parent_id, root)
            return root

        # Parent not yet persisted (out-of-order delivery) or synthetic
        # chain_id with no traces row — use parent_id as the root anchor.
        return parent_id


def _looks_like_uuid(value: str) -> bool:
    try:
        uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


async def _query_parent_root(
    parent_id: str,
    organization_id: Optional[str],
) -> Optional[str]:
    if clickhouse_client._client is None:
        await clickhouse_client.start()
    where = "trace_id = {pid:UUID}"
    params: dict = {"pid": str(parent_id)}
    if organization_id:
        where = "organization_id = {org:UUID} AND " + where
        params["org"] = str(organization_id)
    result = await clickhouse_client._client.query(
        f"SELECT root_trace_id FROM {clickhouse_client.default_table} "
        f"WHERE {where} "
        f"ORDER BY ingested_at DESC LIMIT 1",
        parameters=params,
    )
    if not result.result_rows:
        return None
    root = result.result_rows[0][0]
    if root is None:
        return None
    s = str(root)
    if s == "00000000-0000-0000-0000-000000000000":
        return None
    return s


root_resolver = RootTraceResolver()

__all__ = ["RootTraceResolver", "root_resolver"]
