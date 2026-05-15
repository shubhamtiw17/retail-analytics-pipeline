"""
stream_aggregations.py
──────────────────────
Reads the orders and product_clicks bronze streams and computes
rolling window KPIs that power the real-time dashboard:

  - orders_per_minute      (1-min tumbling window)
  - revenue_last_5_min     (5-min sliding window)
  - top_products_last_hour (1-hour window, top 10 by click count)

Writes results to PostgreSQL every 60 seconds so the dashboard
always has fresh data without hitting Kafka directly.

Design decision: separate aggregation job from ingest job so each
can be restarted independently without affecting the other.
"""

import logging
import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_EXTERNAL_BROKER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    POSTGRES_JDBC_URL,
    POSTGRES_PROPERTIES,
    WINDOW_DURATION,
    WATERMARK_DELAY,
    checkpoint_path,
    bronze_path,
)
from config.schemas import order_schema, click_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stream_aggregations")


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailStreamAggregations")
        .master("local[*]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions",               "4")
        .getOrCreate()
    )


# ── Write aggregation to PostgreSQL ──────────────────────────────────────────

def write_to_postgres(batch_df: DataFrame, batch_id: int, table: str) -> None:
    """foreachBatch sink — writes each micro-batch to a PostgreSQL table."""
    if batch_df.isEmpty():
        return
    count = batch_df.count()
    logger.info(f"[{table}] batch={batch_id} rows={count} → PostgreSQL")
    (
        batch_df.write
        .format("jdbc")
        .option("url",   POSTGRES_JDBC_URL)
        .option("dbtable", table)
        .option("user",    POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver",  POSTGRES_PROPERTIES["driver"])
        .mode("append")
        .save()
    )


# ── Orders per minute ─────────────────────────────────────────────────────────

def orders_per_minute(spark: SparkSession) -> None:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
        .option("subscribe",               "orders")
        .option("startingOffsets",         "latest")
        .load()
    )

    orders = (
        raw
        .select(F.from_json(F.col("value").cast(StringType()), order_schema).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )

    agg = (
        orders
        .groupBy(
            F.window("event_timestamp", "1 minute"),
            "region"
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("revenue"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "region",
            "order_count",
            F.round("revenue", 2).alias("revenue"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            F.current_timestamp().alias("computed_at"),
        )
    )

    (
        agg.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "agg_orders_per_minute"))
        .option("checkpointLocation", checkpoint_path("agg_orders_per_minute"))
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("orders_per_minute stream started")


# ── Top products last hour ────────────────────────────────────────────────────

def top_products(spark: SparkSession) -> None:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_EXTERNAL_BROKER)
        .option("subscribe",               "product_clicks")
        .option("startingOffsets",         "latest")
        .load()
    )

    clicks = (
        raw
        .select(F.from_json(F.col("value").cast(StringType()), click_schema).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withWatermark("event_timestamp", WATERMARK_DELAY)
    )

    agg = (
        clicks
        .groupBy(
            F.window("event_timestamp", "1 hour"),
            "product_id",
            "category",
        )
        .agg(
            F.count("event_id").alias("click_count"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.countDistinct("customer_id").alias("unique_visitors"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "category",
            "click_count",
            F.round("avg_duration_ms", 0).alias("avg_duration_ms"),
            "unique_visitors",
            F.current_timestamp().alias("computed_at"),
        )
    )

    (
        agg.writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "agg_top_products"))
        .option("checkpointLocation", checkpoint_path("agg_top_products"))
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("top_products stream started")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    logger.info("Starting RetailStreamAggregations job")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders_per_minute(spark)
    top_products(spark)

    logger.info("All aggregation streams running — waiting for termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()