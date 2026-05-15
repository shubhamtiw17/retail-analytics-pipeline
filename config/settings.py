import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER          = os.getenv("KAFKA_BROKER",          "localhost:9092")
KAFKA_EXTERNAL_BROKER = os.getenv("KAFKA_EXTERNAL_BROKER", "localhost:9092")

TOPICS = {
    "orders":            "orders",
    "payments":          "payments",
    "customer_events":   "customer_events",
    "inventory_updates": "inventory_updates",
    "product_clicks":    "product_clicks",
    "dlq":               "dead_letter_queue",
}

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "retail-lake")

# ── Local staging (Spark writes here, uploader pushes to MinIO) ───────────────
STAGING_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "staging")

def staging_path(topic: str) -> str:
    return os.path.join(STAGING_DIR, "bronze", topic)

def checkpoint_path(job: str) -> str:
    return os.path.join(STAGING_DIR, "checkpoints", job)

def minio_bronze_prefix(topic: str) -> str:
    return f"bronze/{topic}"

def minio_dlq_prefix() -> str:
    return "dlq"

# ── PostgreSQL ────────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT",     "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "retail_warehouse")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "retail")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail123")

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

POSTGRES_PROPERTIES = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Spark ─────────────────────────────────────────────────────────────────────
MICRO_BATCH_INTERVAL = "30 seconds"
WINDOW_DURATION      = "5 minutes"
WATERMARK_DELAY      = "10 minutes"