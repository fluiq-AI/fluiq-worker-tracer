import asyncio
import json
import logging
import config
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, TopicPartition

from db.clickhouse import clickhouse_client
from db.kafka import kafka_producer
from db.postgresql import postgres_client
from jobs.ingest import ingest_trace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPERATIONS: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {
    "ingest": ingest_trace,
}

# Seek-back retry pacing for transient (connection-class) failures.
_RETRY_INITIAL_S = 1.0
_RETRY_MAX_S = 30.0


def _is_transient(exc: BaseException) -> bool:
    """True for connection-class failures to a backing store (ClickHouse,
    Postgres, Kafka, network). The message itself is fine — the caller seeks
    back to its offset and retries, instead of letting a later ``commit()``
    advance the position past it (which silently loses the message).

    Walks the cause/context chain because drivers wrap the underlying network
    error (e.g. clickhouse_connect raises OperationalError *from* an aiohttp
    ClientConnectorError).
    """
    seen: set[int] = set()
    e: BaseException | None = exc
    depth = 0
    while e is not None and id(e) not in seen and depth < 10:
        seen.add(id(e))
        depth += 1
        if isinstance(e, (OSError, TimeoutError, asyncio.TimeoutError)):
            return True
        mod = type(e).__module__ or ""
        name = type(e).__name__
        if mod.startswith("clickhouse_connect") and name in ("OperationalError", "NetworkError"):
            return True
        if mod.startswith("aiohttp"):
            return True
        if mod.startswith("asyncpg") and (
            "Connect" in name or "TooManyConnections" in name or name == "InterfaceError"
        ):
            return True
        if mod.startswith(("aiokafka", "kafka")) and (
            "Connection" in name or "Timeout" in name or "NodeNotReady" in name
        ):
            return True
        e = e.__cause__ or e.__context__
    return False

async def dispatch(message: dict[str, Any]) -> None:
    operation = message.get("operation", "ingest")
    handler = OPERATIONS.get(operation)
    if handler is None:
        logger.warning("[TRACER] Unknown operation: %s", operation)
        return
    await handler(message)


async def _start_with_retry(attempt: Callable[[], Awaitable[Any]], what: str) -> Any:
    """Bring up one startup dependency, retrying with backoff until reachable.

    Replaces the boot crash-loop (raise → process exit → docker restart →
    repeat until the store is up) with an in-process wait: a worker's job is to
    outlast its stores, not die with them.
    """
    backoff = _RETRY_INITIAL_S
    while True:
        try:
            return await attempt()
        except Exception as exc:
            logger.warning(
                "[TRACER] %s not ready at startup (%s) — retrying in %.1fs",
                what, type(exc).__name__, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _RETRY_MAX_S)


async def consume() -> None:

    async def _consumer_attempt() -> AIOKafkaConsumer:
        # Recreate the consumer per attempt: a failed AIOKafkaConsumer.start()
        # can leave partial client state, so retrying a fresh instance is the
        # only clean path.
        c = AIOKafkaConsumer(
            config.KAFKA_TRACE_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.KAFKA_TRACE_GROUP_ID,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_partition_fetch_bytes=config.KAFKA_MAX_FETCH_BYTES,
            **config.kafka_auth_kwargs(),
        )
        try:
            await c.start()
            return c
        except Exception:
            try:
                await c.stop()
            except Exception:
                pass
            raise

    consumer = await _start_with_retry(_consumer_attempt, "kafka consumer")
    await _start_with_retry(clickhouse_client.start, "clickhouse")
    await _start_with_retry(postgres_client.start, "postgres")
    await _start_with_retry(kafka_producer.start, "kafka producer")
    logger.info(
        "[TRACER] Consuming topic=%s group=%s servers=%s",
        config.KAFKA_TRACE_TOPIC, config.KAFKA_TRACE_GROUP_ID, config.KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        backoff = _RETRY_INITIAL_S
        retries = 0
        async for msg in consumer:
            try:
                await dispatch(msg.value)
                await consumer.commit()
                backoff, retries = _RETRY_INITIAL_S, 0
            except Exception as exc:
                if _is_transient(exc):
                    # Backing store unreachable — the message is fine. Seek back
                    # to it and retry with backoff so a later commit() can never
                    # advance past it (that was silent data loss). Blocks the
                    # partition until the dependency recovers — deliberate for a
                    # persistence worker; Kafka retention holds the backlog.
                    retries += 1
                    consumer.seek(TopicPartition(msg.topic, msg.partition), msg.offset)
                    log = logger.error if retries % 20 == 0 else logger.warning
                    log(
                        "[TRACER] Transient %s at offset=%s partition=%s — seeking back, "
                        "retry #%d in %.1fs",
                        type(exc).__name__, msg.offset, msg.partition, retries, backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _RETRY_MAX_S)
                else:
                    # Poison message — retrying won't help. Skip it DELIBERATELY
                    # by committing past it (previously this skip happened as an
                    # accident of the next successful commit).
                    logger.exception(
                        "[TRACER] Failed to process message offset=%s partition=%s — skipping",
                        msg.offset, msg.partition,
                    )
                    try:
                        await consumer.commit()
                    except Exception:
                        logger.warning(
                            "[TRACER] Commit after poison skip failed; message may re-deliver on restart",
                        )
                    backoff, retries = _RETRY_INITIAL_S, 0
    finally:
        await consumer.stop()
        await kafka_producer.stop()
        await clickhouse_client.stop()
        await postgres_client.stop()


def main() -> None:
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("[TRACER] Shutdown requested")


if __name__ == "__main__":
    main()