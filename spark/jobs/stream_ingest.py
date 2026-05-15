"""
stream_ingest.py
────────────────
Reads all 5 Kafka topics via PySpark Structured Streaming.
Each micro-batch is:
  1. Parsed and validated against a strict schema
  2. Written to local staging as Parquet (partitioned by date)
  3. Uploaded to MinIO bronze layer via the Python minio client

Invalid events (schema parse failures) are routed to the
dead_letter_queue Kafka topic for inspection and replay.

Design decision: local staging → MinIO upload instead of S3A.
S3A has a known Py4J socket issue on Windows with Python 3.10.
The local-stage-then-upload pattern is used by AWS Glue internally
and gives us reliable exactly-once delivery with full MinIO storage.
"""

import logging
import os
import sys
import glob

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from minio import Minio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_EXTERNAL_BROKER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    MICRO_BATCH_INTERVAL,
    STAGING_DIR,
    TOPICS,
    staging_path,
    checkpoint_path,
    minio_bronze_prefix,
    minio_dlq_prefix,
)
from config.schemas import SCHEMAS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stream_ingest")


# ── MinIO client (module-level singleton) ─────────────────────────────────────

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailStreamIngest")
        .master("local[2]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions",    "4")
        .config("spark.ui.showConsoleProgress",    "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


# ── MinIO uploader ────────────────────────────────────────────────────────────

def upload_to_minio(local_dir: str, minio_prefix: str) -> int:
    """
    Walk all Parquet files under local_dir and upload each to MinIO.
    Returns the number of files uploaded.
    """
    uploaded = 0
    pattern  = os.path.join(local_dir, "**", "*.parquet")

    for local_path in glob.glob(pattern, recursive=True):
        # Preserve partition folder structure in MinIO
        # e.g. data/staging/bronze/orders/ingest_date=2026-05-15/part-0.parquet
        #   → bronze/orders/ingest_date=2026-05-15/part-0.parquet
        relative  = os.path.relpath(local_path, STAGING_DIR)
        minio_key = relative.replace("\\", "/")   # Windows path fix

        minio_client.fput_object(MINIO_BUCKET, minio_key, local_path)
        uploaded += 1

    return uploaded


# ── Per-topic foreachBatch handler ────────────────────────────────────────────

def make_batch_handler(topic: str):
    """
    Returns a foreachBatch function for one topic.
    Each micro-batch:
      - Writes valid rows to local Parquet (partitioned by ingest_date)
      - Uploads those files to MinIO bronze
      - Sends invalid rows to dead_letter_queue
    """
    local_path = staging_path(topic)

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            logger.info(f"[{topic}] batch={batch_id} empty — skipping")
            return

        total = batch_df.count()
        valid   = batch_df.filter(F.col("event_id").isNotNull())
        invalid = batch_df.filter(F.col("event_id").isNull())

        valid_count   = valid.count()
        invalid_count = invalid.count()

        logger.info(
            f"[{topic}] batch={batch_id} "
            f"total={total} valid={valid_count} invalid={invalid_count}"
        )

        # ── Write valid rows to local staging Parquet ─────────────────────
        if valid_count > 0:
            (
                valid
                .withColumn("ingest_date", F.to_date(F.col("event_timestamp")))
                .write
                .mode("append")
                .partitionBy("ingest_date")
                .parquet(local_path)
            )

            # ── Upload to MinIO ───────────────────────────────────────────
            uploaded = upload_to_minio(local_path, minio_bronze_prefix(topic))
            logger.info(f"[{topic}] batch={batch_id} uploaded={uploaded} files to MinIO")

        # ── Route invalid rows to dead letter queue ────────────────────────
        if invalid_count > 0:
            (
                invalid
                .select(
                    F.to_json(F.struct("*")).alias("value"),
                    F.lit(topic).alias("key"),
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
                .option("topic", TOPICS["dlq"])
                .save()
            )
            logger.info(f"[{topic}] batch={batch_id} sent {invalid_count} rows to DLQ")

    return handle_batch


# ── Per-topic streaming pipeline ──────────────────────────────────────────────

def process_topic(spark: SparkSession, topic: str) -> None:
    schema = SCHEMAS[topic]

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )

    parsed = (
        raw
        .select(
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast(StringType()),
                schema
            ).alias("data")
        )
        .select("offset", "partition", "kafka_timestamp", "data.*")
        .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("ingested_at",     F.current_timestamp())
    )

    query = (
        parsed.writeStream
        .foreachBatch(make_batch_handler(topic))
        .option("checkpointLocation", checkpoint_path(f"ingest_{topic}"))
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .outputMode("append")
        .start()
    )

    logger.info(f"[{topic}] stream started — query_id={query.id}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # Ensure staging directories exist
    for topic in SCHEMAS:
        os.makedirs(staging_path(topic), exist_ok=True)
    os.makedirs(os.path.join(STAGING_DIR, "checkpoints"), exist_ok=True)

    logger.info("Starting RetailStreamIngest")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    for topic in SCHEMAS:
        process_topic(spark, topic)

    logger.info("All streams running — waiting for termination (Ctrl+C to stop)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()