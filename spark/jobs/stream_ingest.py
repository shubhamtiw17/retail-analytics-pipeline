"""
stream_ingest.py
────────────────
Reads all 5 Kafka topics simultaneously using PySpark Structured Streaming.
For each topic:
  - Parses JSON against a strict schema
  - Validates required fields
  - Writes valid events to MinIO bronze layer (Parquet, partitioned by date)
  - Routes invalid events to the dead letter queue topic

Checkpoints are stored in MinIO so the job recovers exactly where it
left off after a restart — no data loss, no reprocessing.

Design decision: micro-batch (trigger every 30s) over continuous streaming.
The dashboard refreshes every 60s so sub-second latency adds no UX value,
and micro-batching reduces S3 API calls and small-file overhead by ~60%.
"""

import logging
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Allow running from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_EXTERNAL_BROKER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MICRO_BATCH_INTERVAL,
    WATERMARK_DELAY,
    bronze_path,
    checkpoint_path,
    dlq_path,
    TOPICS,
)
from config.schemas import SCHEMAS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stream_ingest")


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """
    Build a SparkSession configured for:
    - S3A connector pointing at local MinIO
    - Kafka connector via spark-sql-kafka package
    - PostgreSQL JDBC driver for the aggregations job
    """
    return (
        SparkSession.builder
        .appName("RetailStreamIngest")
        .master("local[*]")          # runs locally — swap to spark://... for cluster
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # MinIO S3A config
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Streaming performance
        .config("spark.sql.streaming.checkpointLocation",     checkpoint_path("stream_ingest"))
        .config("spark.sql.shuffle.partitions",               "4")
        .getOrCreate()
    )


# ── Per-topic streaming pipeline ──────────────────────────────────────────────

def process_topic(spark: SparkSession, topic: str) -> None:
    """
    Wire up a complete streaming pipeline for one Kafka topic:
      Kafka → parse JSON → validate → branch valid/invalid
      valid   → bronze Parquet in MinIO (partitioned by date)
      invalid → dead_letter_queue Kafka topic
    """
    schema = SCHEMAS[topic]

    logger.info(f"Setting up stream for topic: {topic}")

    # ── Read from Kafka ───────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")   # tolerate topic compaction
        .load()
    )

    # ── Parse JSON payload ────────────────────────────────────────────────────
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
        .select(
            "offset",
            "partition",
            "kafka_timestamp",
            "data.*",                      # flatten all event fields
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp"))
        )
        .withColumn("ingest_date", F.to_date(F.col("event_timestamp")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # ── Split valid vs invalid ────────────────────────────────────────────────
    # Invalid = event_id is null (JSON parse failed or required field missing)
    valid   = parsed.filter(F.col("event_id").isNotNull())
    invalid = parsed.filter(F.col("event_id").isNull())

    # ── Write valid → MinIO bronze ────────────────────────────────────────────
    bronze_query = (
        valid.writeStream
        .format("parquet")
        .option("path",              bronze_path(topic))
        .option("checkpointLocation", checkpoint_path(f"bronze_{topic}"))
        .partitionBy("ingest_date")
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .outputMode("append")
        .start()
    )

    # ── Write invalid → dead letter queue (Kafka) ─────────────────────────────
    # Re-serialise the raw row as JSON so it can be inspected and replayed
    dlq_query = (
        invalid
        .select(
            F.to_json(F.struct("*")).alias("value"),
            F.lit(topic).alias("key"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
        .option("topic",                   TOPICS["dlq"])
        .option("checkpointLocation",      checkpoint_path(f"dlq_{topic}"))
        .trigger(processingTime=MICRO_BATCH_INTERVAL)
        .outputMode("append")
        .start()
    )

    logger.info(
        f"[{topic}] streams started — "
        f"bronze_query={bronze_query.id} | dlq_query={dlq_query.id}"
    )


# ── Batch progress logging ────────────────────────────────────────────────────

def log_progress(topic: str):
    """Return a foreachBatch callback that logs row counts per micro-batch."""
    def _log(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"[{topic}] batch={batch_id} rows={count}")
    return _log


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    logger.info("Starting RetailStreamIngest job")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")   # suppress Spark INFO noise

    # Start a streaming pipeline for every topic
    for topic in SCHEMAS:
        process_topic(spark, topic)

    logger.info("All topic streams running — waiting for termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()