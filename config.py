import os
import ssl
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TRACE_TOPIC = os.getenv("KAFKA_TRACE_TOPIC")
KAFKA_TRACE_PERSISTED_TOPIC = os.getenv("KAFKA_TRACE_PERSISTED_TOPIC")
KAFKA_TRACE_GROUP_ID=os.getenv("KAFKA_TRACE_GROUP_ID")
# PLAINTEXT (local docker) | SASL_SSL (AWS MSK SASL/SCRAM)
KAFKA_SECURITY_PROTOCOL=os.getenv("KAFKA_SECURITY_PROTOCOL")
KAFKA_SASL_MECHANISM=os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_SASL_USERNAME=os.getenv("KAFKA_SASL_USERNAME")
KAFKA_SASL_PASSWORD=os.getenv("KAFKA_SASL_PASSWORD")

# Match the API's Kafka sizing: trace events can be a few MB, so the consumer's
# per-partition fetch ceiling and the re-publish producer's request size must be
# raised above the ~1MB default. Keep <= broker message.max.bytes / fetch sizes.
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", str(10 * 1024 * 1024)))
KAFKA_MAX_FETCH_BYTES = int(os.getenv("KAFKA_MAX_FETCH_BYTES", str(10 * 1024 * 1024)))


def kafka_auth_kwargs() -> dict:
    """aiokafka security kwargs derived from env, shared by consumer + producer.

    PLAINTEXT (default, local docker-compose) → no auth.
    SASL_SSL → SCRAM-SHA-512 username/password over TLS (AWS MSK). MSK broker
    certs chain to Amazon Trust Services (in the default CA bundle), so no CA
    file is needed.
    """
    protocol = (KAFKA_SECURITY_PROTOCOL or "PLAINTEXT").upper()
    if protocol == "SASL_SSL":
        return {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_SASL_USERNAME,
            "sasl_plain_password": KAFKA_SASL_PASSWORD,
            "ssl_context": ssl.create_default_context(),
        }
    return {"security_protocol": "PLAINTEXT"}

POSTGRES_DSN = os.getenv("POSTGRES_DSN")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX"))
POSTGRES_SSL_CA_FILE = os.getenv("POSTGRES_SSL_CA_FILE")

POSTGRES_USER_TABLE = os.getenv("POSTGRES_USER_TABLE")
POSTGRES_ORG_TABLE = os.getenv("POSTGRES_ORG_TABLE")
POSTGRES_REVOKED_TOKEN_TABLE = os.getenv("POSTGRES_REVOKED_TOKEN_TABLE")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")
CLICKHOUSE_TRACE_TABLE = os.getenv("CLICKHOUSE_TRACE_TABLE")
CLICKHOUSE_TRACE_COSTS_TABLE = os.getenv("CLICKHOUSE_TRACE_COSTS_TABLE")
CLICKHOUSE_EVALUATIONS_TABLE = os.getenv("CLICKHOUSE_EVALUATIONS_TABLE")
