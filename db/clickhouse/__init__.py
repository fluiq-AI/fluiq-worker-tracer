import logging
import config
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """Async ClickHouse client for storing trace records."""

    def __init__(
        self,
        host: str = config.CLICKHOUSE_HOST,
        port: int = config.CLICKHOUSE_PORT,
        username: str = config.CLICKHOUSE_USER,
        password: str = config.CLICKHOUSE_PASSWORD,
        database: str = config.CLICKHOUSE_DATABASE,
        default_table: str = config.CLICKHOUSE_TRACE_TABLE,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.default_table = default_table
        self._client: Optional[AsyncClient] = None

    async def start(self) -> None:
        if self._client is not None:
            return
        self._client = await clickhouse_connect.get_async_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
        logger.info("[CLICKHOUSE] Client started: %s:%s/%s", self.host, self.port, self.database)

    async def stop(self) -> None:
        if self._client is None:
            return
        await self._client.close()
        self._client = None
        logger.info("[CLICKHOUSE] Client stopped")

    async def insert_trace(
        self,
        trace: dict[str, Any],
        table: Optional[str] = None,
    ) -> None:
        if self._client is None:
            await self.start()
        target = table or self.default_table
        # Retention window (days) stamped by the API from the org's tier:
        # Free = 14, paid = 36500 (~never). Fall back to the "keep forever"
        # sentinel if the field is missing so we never delete unexpectedly.
        retention_days = int(trace.get("retention_days") or 36500)
        # Denormalized agent-run identity: is_root (1 = own-root or orphan root)
        # + agent_key/agent_kind, stamped so reads skip the whole-org NOT IN
        # scan and JSON extraction. Default to root/empty if a producer omits
        # them (biases toward visibility, matching the orphan-root philosophy).
        is_root = int(trace.get("is_root", 1))
        agent_key = str(trace.get("agent_key") or "")
        agent_kind = str(trace.get("agent_kind") or "")
        await self._client.insert(
            target,
            [[
                trace.get("organization_id"),
                trace.get("api_key_prefix"),
                trace.get("trace_id"),
                trace.get("root_trace_id") or trace.get("trace_id"),
                trace.get("event") or {},
                retention_days,
                is_root,
                agent_key,
                agent_kind,
            ]],
            column_names=[
                "organization_id",
                "api_key_prefix",
                "trace_id",
                "root_trace_id",
                "event",
                "retention_days",
                "is_root",
                "agent_key",
                "agent_kind",
            ],
        )

    async def insert_cost(
        self,
        record: dict[str, Any],
        table: str = config.CLICKHOUSE_TRACE_COSTS_TABLE,
    ) -> None:
        if self._client is None:
            await self.start()
        await self._client.insert(
            table,
            [[
                record.get("organization_id"),
                record.get("api_key_prefix"),
                record.get("trace_id"),
                record.get("root_trace_id") or record.get("trace_id"),
                record.get("provider") or "",
                record.get("model") or "",
                record.get("modality") or "Text",
                int(record.get("input_tokens") or 0),
                int(record.get("cached_input_tokens") or 0),
                int(record.get("output_tokens") or 0),
                record.get("input_cost") or 0,
                record.get("cached_input_cost") or 0,
                record.get("output_cost") or 0,
                record.get("total_cost") or 0,
                record.get("currency") or "USD",
                1 if record.get("long_context") else 0,
            ]],
            column_names=[
                "organization_id",
                "api_key_prefix",
                "trace_id",
                "root_trace_id",
                "provider",
                "model",
                "modality",
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "input_cost",
                "cached_input_cost",
                "output_cost",
                "total_cost",
                "currency",
                "long_context",
            ],
        )


clickhouse_client = ClickHouseClient()

__all__ = [
    "ClickHouseClient",
    "clickhouse_client",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_DATABASE",
    "CLICKHOUSE_TRACE_TABLE",
    "CLICKHOUSE_TRACE_COSTS_TABLE",
]
