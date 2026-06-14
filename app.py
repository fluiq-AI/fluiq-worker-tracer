import asyncio
import json
import logging
import config
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer

from db.clickhouse import clickhouse_client
from db.kafka import kafka_producer
from db.postgresql import postgres_client
from jobs.ingest import ingest_trace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPERATIONS: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {
    "ingest": ingest_trace,
}

async def dispatch(message: dict[str, Any]) -> None:
    operation = message.get("operation", "ingest")
    handler = OPERATIONS.get(operation)
    if handler is None:
        logger.warning("[TRACER] Unknown operation: %s", operation)
        return
    await handler(message)


async def consume() -> None:

    consumer = AIOKafkaConsumer(
        config.KAFKA_TRACE_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=config.KAFKA_TRACE_GROUP_ID,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        **config.kafka_auth_kwargs(),
    )
    await consumer.start()
    await clickhouse_client.start()
    await postgres_client.start()
    await kafka_producer.start()
    logger.info(
        "[TRACER] Consuming topic=%s group=%s servers=%s",
        config.KAFKA_TRACE_TOPIC, config.KAFKA_TRACE_GROUP_ID, config.KAFKA_BOOTSTRAP_SERVERS,
    )
    try:
        async for msg in consumer:
            try:
                await dispatch(msg.value)
                await consumer.commit()
            except Exception:
                logger.exception(
                    "[TRACER] Failed to process message offset=%s partition=%s",
                    msg.offset, msg.partition,
                )
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