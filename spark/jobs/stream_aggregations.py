"""
stream_aggregations.py
──────────────────────
Reads orders and product_clicks from Kafka and computes
rolling window KPIs written directly to PostgreSQL via psycopg2.

Uses psycopg2 instead of JDBC in foreachBatch to avoid the
S3A Py4J socket issue — psycopg2 is a pure Python driver
that works reliably on Windows with PySpark local mode.
"""

import logging
import os
import sys
import psycopg2
import psycopg2.extras
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.settings import (
    KAFKA_EXTERNAL_BROKER,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    WATERMARK_DELAY,
    checkpoint_path,
    STAGING_DIR,
)
from config.schemas import order_schema, click_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("stream_aggregations")


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def write_orders_agg(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    logger.info(f"[orders_agg] batch={batch_id} rows={len(rows)} → PostgreSQL")

    sql = """
        INSERT INTO agg_orders_per_minute
            (window_start, window_end, region, order_count, revenue, avg_order_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, (
                    row.window_start,
                    row.window_end,
                    row.region,
                    row.order_count,
                    float(row.revenue) if row.revenue else 0.0,
                    float(row.avg_order_value) if row.avg_order_value else 0.0,
                ))
        conn.commit()
    finally:
        conn.close()


def write_products_agg(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    logger.info(f"[top_products] batch={batch_id} rows={len(rows)} → PostgreSQL")

    sql = """
        INSERT INTO agg_top_products
            (window_start, window_end, product_id, category,
             click_count, avg_duration_ms, unique_visitors)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, (
                    row.window_start,
                    row.window_end,
                    row.product_id,
                    row.category,
                    row.click_count,
                    float(row.avg_duration_ms) if row.avg_duration_ms else 0.0,
                    row.unique_visitors,
                ))
        conn.commit()
    finally:
        conn.close()


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailStreamAggregations")
        .master("local[2]")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions",    "4")
        .config("spark.ui.showConsoleProgress",    "false")
        .getOrCreate()
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
        .groupBy(F.window("event_timestamp", "1 minute"), "region")
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
        )
    )

    (
        agg.writeStream
        .foreachBatch(write_orders_agg)
        .option("checkpointLocation", checkpoint_path("agg_orders"))
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("orders_per_minute stream started")


# ── Top products ──────────────────────────────────────────────────────────────

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
        .groupBy(F.window("event_timestamp", "1 hour"), "product_id", "category")
        .agg(
            F.count("event_id").alias("click_count"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.approx_count_distinct("customer_id").alias("unique_visitors"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "category",
            "click_count",
            F.round("avg_duration_ms", 0).alias("avg_duration_ms"),
            "unique_visitors",
        )
    )

    (
        agg.writeStream
        .foreachBatch(write_products_agg)
        .option("checkpointLocation", checkpoint_path("agg_products"))
        .trigger(processingTime="60 seconds")
        .outputMode("update")
        .start()
    )
    logger.info("top_products stream started")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    os.makedirs(os.path.join(STAGING_DIR, "checkpoints"), exist_ok=True)

    logger.info("Starting RetailStreamAggregations")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders_per_minute(spark)
    top_products(spark)

    logger.info("All aggregation streams running — waiting for termination")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()