"""
retail_pipeline.py
──────────────────
Daily Airflow DAG that loads bronze Parquet files into the
PostgreSQL star schema incrementally, then triggers dbt.

Task order:
  load_orders → load_payments → load_clicks
       ↓              ↓              ↓
  update_dim_customer ← (all three feed this)
       ↓
  update_dim_product
       ↓
  run_dbt_models
       ↓
  validate_quality
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2
import pandas as pd
import pyarrow.parquet as pq
import os
import glob
import logging

logger = logging.getLogger(__name__)

# ── Connection ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host="postgres",
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB",  "retail_warehouse"),
        user=os.getenv("POSTGRES_USER",  "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail123"),
    )

STAGING_BASE = "/opt/airflow/staging"

# ── Loaders ───────────────────────────────────────────────────────────────────

def load_orders(**context):
    conn = get_conn()
    staging = os.path.join(STAGING_BASE, "bronze", "orders")

    if not os.path.exists(staging):
        logger.info("No orders staging data yet")
        conn.close()
        return

    files = glob.glob(os.path.join(staging, "**", "*.parquet"), recursive=True)
    if not files:
        logger.info("No parquet files found for orders")
        conn.close()
        return

    logger.info(f"Found {len(files)} parquet files for orders")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.dropna(subset=["order_id", "customer_id"])
    df["order_date"] = pd.to_datetime(df["event_timestamp"]).dt.date
    # item_count from items column (list) — handle safely
    df["item_count"] = df["items"].apply(lambda x: len(x) if isinstance(x, list) else 0)

    inserted = 0
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO fact_orders
                    (order_id, customer_id, order_date, total_amount,
                     item_count, status, device, region, currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                str(row["order_id"]),
                str(row["customer_id"]),
                row["order_date"],
                float(row["total_amount"]) if pd.notna(row.get("total_amount")) else 0.0,
                int(row["item_count"]),
                str(row.get("status", "")),
                str(row.get("device", "")),
                str(row.get("region", "")),
                str(row.get("currency", "USD")),
            ))
            inserted += cur.rowcount

        cur.execute("""
            UPDATE etl_watermarks
            SET last_loaded_at = NOW(), rows_loaded = rows_loaded + %s, updated_at = NOW()
            WHERE table_name = 'fact_orders'
        """, (inserted,))

    conn.commit()
    conn.close()
    logger.info(f"load_orders: inserted {inserted} rows")


def load_payments(**context):
    conn = get_conn()
    staging = os.path.join(STAGING_BASE, "bronze", "payments")

    if not os.path.exists(staging):
        logger.info("No payments staging data yet")
        conn.close()
        return

    files = glob.glob(os.path.join(staging, "**", "*.parquet"), recursive=True)
    if not files:
        conn.close()
        return

    logger.info(f"Found {len(files)} parquet files for payments")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.dropna(subset=["payment_id", "customer_id"])
    df["payment_date"] = pd.to_datetime(df["event_timestamp"]).dt.date

    inserted = 0
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO fact_payments
                    (payment_id, order_id, customer_id, payment_date,
                     amount, method, status, currency, gateway_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (payment_id) DO NOTHING
            """, (
                str(row["payment_id"]),
                str(row.get("order_id", "")),
                str(row["customer_id"]),
                row["payment_date"],
                float(row["amount"]) if pd.notna(row.get("amount")) else 0.0,
                str(row.get("method", "")),
                str(row.get("status", "")),
                str(row.get("currency", "USD")),
                str(row.get("gateway_ref", "")),
            ))
            inserted += cur.rowcount

        cur.execute("""
            UPDATE etl_watermarks
            SET last_loaded_at = NOW(), rows_loaded = rows_loaded + %s, updated_at = NOW()
            WHERE table_name = 'fact_payments'
        """, (inserted,))

    conn.commit()
    conn.close()
    logger.info(f"load_payments: inserted {inserted} rows")


def load_clicks(**context):
    conn = get_conn()
    staging = os.path.join(STAGING_BASE, "bronze", "product_clicks")

    if not os.path.exists(staging):
        logger.info("No clicks staging data yet")
        conn.close()
        return

    files = glob.glob(os.path.join(staging, "**", "*.parquet"), recursive=True)
    if not files:
        conn.close()
        return

    logger.info(f"Found {len(files)} parquet files for clicks")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.dropna(subset=["event_id", "customer_id", "product_id"])
    df["click_date"] = pd.to_datetime(df["event_timestamp"]).dt.date

    inserted = 0
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO fact_product_clicks
                    (click_id, customer_id, product_id, click_date,
                     session_id, device, referrer, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (click_id) DO NOTHING
            """, (
                str(row["event_id"]),
                str(row["customer_id"]),
                str(row["product_id"]),
                row["click_date"],
                str(row.get("session_id", "")),
                str(row.get("device", "")),
                str(row.get("referrer", "")),
                int(row["duration_ms"]) if pd.notna(row.get("duration_ms")) else 0,
            ))
            inserted += cur.rowcount

        cur.execute("""
            UPDATE etl_watermarks
            SET last_loaded_at = NOW(), rows_loaded = rows_loaded + %s, updated_at = NOW()
            WHERE table_name = 'fact_product_clicks'
        """, (inserted,))

    conn.commit()
    conn.close()
    logger.info(f"load_clicks: inserted {inserted} rows")


def update_dim_customer(**context):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_customer
                (customer_id, first_seen_at, last_seen_at, total_orders, preferred_device)
            SELECT
                customer_id,
                MIN(created_at)                                     AS first_seen_at,
                MAX(created_at)                                     AS last_seen_at,
                COUNT(*)                                            AS total_orders,
                MODE() WITHIN GROUP (ORDER BY device)               AS preferred_device
            FROM fact_orders
            GROUP BY customer_id
            ON CONFLICT (customer_id) DO UPDATE SET
                last_seen_at    = EXCLUDED.last_seen_at,
                total_orders    = EXCLUDED.total_orders,
                preferred_device = EXCLUDED.preferred_device,
                updated_at      = NOW()
        """)
    conn.commit()
    conn.close()
    logger.info("update_dim_customer: done")


def update_dim_product(**context):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dim_product
                (product_id, first_clicked, last_clicked,
                 total_clicks, avg_duration_ms)
            SELECT
                product_id,
                MIN(created_at)         AS first_clicked,
                MAX(created_at)         AS last_clicked,
                COUNT(*)                AS total_clicks,
                AVG(duration_ms)        AS avg_duration_ms
            FROM fact_product_clicks
            GROUP BY product_id
            ON CONFLICT (product_id) DO UPDATE SET
                last_clicked    = EXCLUDED.last_clicked,
                total_clicks    = EXCLUDED.total_clicks,
                avg_duration_ms = EXCLUDED.avg_duration_ms,
                updated_at      = NOW()
        """)
    conn.commit()
    conn.close()
    logger.info("update_dim_product: done")


def validate_quality(**context):
    conn = get_conn()
    issues = []

    with conn.cursor() as cur:
        # Check 1: orders with no matching customer in dim
        cur.execute("""
            SELECT COUNT(*) FROM fact_orders fo
            LEFT JOIN dim_customer dc ON fo.customer_id = dc.customer_id
            WHERE dc.customer_id IS NULL
        """)
        orphan_orders = cur.fetchone()[0]
        if orphan_orders > 0:
            issues.append(f"orphan orders (no dim_customer): {orphan_orders}")

        # Check 2: negative revenue
        cur.execute("SELECT COUNT(*) FROM fact_orders WHERE total_amount < 0")
        neg_revenue = cur.fetchone()[0]
        if neg_revenue > 0:
            issues.append(f"negative revenue rows: {neg_revenue}")

        # Check 3: payments with invalid status
        cur.execute("""
            SELECT COUNT(*) FROM fact_payments
            WHERE status NOT IN ('success', 'failed', 'pending')
        """)
        bad_status = cur.fetchone()[0]
        if bad_status > 0:
            issues.append(f"payments with invalid status: {bad_status}")

        # Log results
        cur.execute("""
            INSERT INTO pipeline_runs (job_name, finished_at, status, rows_processed, error_message)
            VALUES ('validate_quality', NOW(), %s, 0, %s)
        """, (
            'failed' if issues else 'success',
            '; '.join(issues) if issues else None,
        ))

    conn.commit()
    conn.close()

    if issues:
        logger.warning(f"Data quality issues found: {issues}")
    else:
        logger.info("validate_quality: all checks passed ✅")


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "retail-eng",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="Daily ETL: bronze Parquet → star schema → dbt",
    schedule_interval="0 3 * * *",     # 3am daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail", "etl", "star-schema"],
) as dag:

    t_load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    t_load_payments = PythonOperator(
        task_id="load_payments",
        python_callable=load_payments,
    )

    t_load_clicks = PythonOperator(
        task_id="load_clicks",
        python_callable=load_clicks,
    )

    t_dim_customer = PythonOperator(
        task_id="update_dim_customer",
        python_callable=update_dim_customer,
    )

    t_dim_product = PythonOperator(
        task_id="update_dim_product",
        python_callable=update_dim_product,
    )

    t_validate = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    [t_load_orders, t_load_payments, t_load_clicks] >> t_dim_customer
    t_dim_customer >> t_dim_product >> t_validate