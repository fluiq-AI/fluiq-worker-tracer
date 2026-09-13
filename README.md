# fluiq-worker-tracer

Kafka consumer that persists agent and LLM spans to ClickHouse, then fans the
run out to the evaluation and security workers.

> **Status: archived.** Part of [Fluiq](https://github.com/fluiq-AI), which ran
> from 10 April to September 2026 and never found customers. The hosted service
> is shut down. MIT, unmaintained, fork freely.

## Where it sits

```
SDK ──► fluiq-api POST /ingest ──► Kafka: fluiq.traces
                                        │
                                        ▼
                              fluiq-worker-tracer  ◄── you are here
                                        │
                          ┌─────────────┴─────────────┐
                          ▼                           ▼
                     ClickHouse              Kafka: fluiq.traces.persisted
                  traces, trace_costs                 │
                                        ┌─────────────┴─────────────┐
                                        ▼                           ▼
                              fluiq-worker-evaluator      fluiq-worker-security
```

It is the only writer on the hot ingest path, so everything here is tuned for
never losing a span and never blocking the API.

| | |
|---|---|
| **Consumes** | `KAFKA_TRACE_TOPIC` as group `KAFKA_TRACE_GROUP_ID` |
| **Produces** | `KAFKA_TRACE_PERSISTED_TOPIC` |
| **Writes** | ClickHouse `traces`, `trace_costs`; reads org tier from Postgres |

## What it does per message

1. Normalizes the span batch and resolves the org from the API key.
2. Computes token cost per span from the model price table.
3. Stamps `retention_days` on every row from the org's tier — retention is a
   per-row ClickHouse TTL decided at ingest, not a background sweep, so changing
   a plan never rewrites history.
4. Inserts to ClickHouse.
5. Publishes the run to the persisted topic so eval and security can pick it up
   independently.

## Configuration

All environment variables, read in [`config.py`](config.py). Nothing has a
useful fallback and several are `int()`-wrapped, so a missing value raises
`TypeError` at import.

```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_TRACE_TOPIC=fluiq.traces
KAFKA_TRACE_PERSISTED_TOPIC=fluiq.traces.persisted
KAFKA_TRACE_GROUP_ID=fluiq-tracer
KAFKA_MAX_REQUEST_SIZE=10485760
KAFKA_MAX_FETCH_BYTES=10485760

POSTGRES_DSN=postgresql://fluiq:fluiq@postgres:5432/fluiq
POSTGRES_POOL_MIN=1
POSTGRES_POOL_MAX=5
POSTGRES_USER_TABLE=users
POSTGRES_ORG_TABLE=organizations
POSTGRES_REVOKED_TOKEN_TABLE=revoked_refresh_tokens

CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=fluiq
CLICKHOUSE_TRACE_TABLE=fluiq.traces
CLICKHOUSE_TRACE_COSTS_TABLE=fluiq.trace_costs
CLICKHOUSE_EVALUATIONS_TABLE=fluiq.evaluations
```

## Running

The data stores live in [fluiq-api](https://github.com/fluiq-AI/fluiq-api)'s
compose file. Bring those up first, then:

```bash
cp .env.example .env.development   # if present; otherwise use the block above
docker build -t fluiq-tracer .
docker run --network fluiq-ai --env-file .env.development fluiq-tracer
```

Or directly:

```bash
pip install -r requirements.txt
python app.py
```

## Notable decisions

**A transient failure seeks back rather than committing.** On a retryable error
the consumer rewinds to the failed message's own offset and re-consumes it. The
obvious alternative — commit and move on — silently drops spans on a ClickHouse
blip, which is the one thing a trace store must never do.

**Startup retries instead of crash-looping.** `_start_with_retry` wraps both
consumer and producer start, recreating the `AIOKafkaConsumer` on each attempt,
because a failed `start()` leaves the object unusable. Before this, a broker that
was slow to come up under compose would put the container in a restart loop that
outlived the broker's recovery.

## Licence

MIT. See [LICENSE](LICENSE).
