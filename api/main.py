"""
FastAPI service exposing the retail warehouse data via REST endpoints.
All endpoints read from PostgreSQL mart and fact tables.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from datetime import date
from typing import Optional

load_dotenv()

app = FastAPI(
    title="Retail Analytics API",
    description="REST API for the retail analytics warehouse",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "retail_warehouse"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail123"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )

def execute(sql: str, params=None) -> list:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    """Check connectivity to PostgreSQL."""
    try:
        execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


# ── KPIs ──────────────────────────────────────────────────────────────────────

@app.get("/kpis/summary", tags=["KPIs"])
def kpi_summary():
    """Top-level KPIs: total orders, revenue, customers, products."""
    rows = execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_orders)          AS total_orders,
            (SELECT ROUND(SUM(total_amount)::NUMERIC,2)
             FROM fact_orders)                          AS total_revenue,
            (SELECT COUNT(*) FROM dim_customer)         AS total_customers,
            (SELECT COUNT(*) FROM dim_product)          AS total_products,
            (SELECT COUNT(*) FROM fact_product_clicks)  AS total_clicks
    """)
    return rows[0]


@app.get("/kpis/revenue/daily", tags=["KPIs"])
def daily_revenue(
    days: int = Query(default=30, ge=1, le=365, description="Number of days to look back")
):
    """Daily revenue grouped by date, last N days."""
    return execute("""
        SELECT
            order_date,
            SUM(gross_revenue)      AS revenue,
            SUM(total_orders)       AS orders,
            SUM(unique_customers)   AS customers
        FROM dbt_marts_marts.mart_daily_revenue
        WHERE order_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY order_date
        ORDER BY order_date DESC
    """, (days,))


@app.get("/kpis/revenue/by-region", tags=["KPIs"])
def revenue_by_region():
    """Total revenue broken down by region."""
    return execute("""
        SELECT
            region,
            SUM(gross_revenue)      AS total_revenue,
            SUM(total_orders)       AS total_orders
        FROM dbt_marts_marts.mart_daily_revenue
        GROUP BY region
        ORDER BY total_revenue DESC
    """)


# ── Products ──────────────────────────────────────────────────────────────────

@app.get("/products/top", tags=["Products"])
def top_products(
    limit: int = Query(default=10, ge=1, le=100),
    category: Optional[str] = Query(default=None),
):
    """Top products by total clicks."""
    base = """
        SELECT
            product_id,
            category,
            SUM(total_clicks)       AS total_clicks,
            SUM(unique_visitors)    AS unique_visitors,
            ROUND(AVG(avg_session_duration_sec)::NUMERIC, 1) AS avg_duration_sec
        FROM dbt_marts_marts.mart_product_performance
    """
    if category:
        base += " WHERE category = %(category)s"
        base += " GROUP BY product_id, category ORDER BY total_clicks DESC LIMIT %(limit)s"
        return execute(base, {"category": category, "limit": limit})
    else:
        base += " GROUP BY product_id, category ORDER BY total_clicks DESC LIMIT %(limit)s"
        return execute(base, {"limit": limit})


# ── Customers ─────────────────────────────────────────────────────────────────

@app.get("/customers/segments", tags=["Customers"])
def customer_segments():
    """Customer count and average LTV by segment."""
    return execute("""
        SELECT
            customer_segment,
            COUNT(*)                            AS customer_count,
            ROUND(AVG(lifetime_value)::NUMERIC,2) AS avg_ltv,
            ROUND(SUM(lifetime_value)::NUMERIC,2) AS total_revenue
        FROM dbt_marts_marts.mart_customer_summary
        GROUP BY customer_segment
        ORDER BY avg_ltv DESC
    """)


@app.get("/customers/{customer_id}", tags=["Customers"])
def customer_detail(customer_id: str):
    """Full profile for a single customer."""
    rows = execute("""
        SELECT * FROM dbt_marts_marts.mart_customer_summary
        WHERE customer_id = %(customer_id)s
    """, {"customer_id": customer_id})
    if not rows:
        raise HTTPException(status_code=404, detail="Customer not found")
    return rows[0]


# ── Pipeline ──────────────────────────────────────────────────────────────────

@app.get("/pipeline/health", tags=["Pipeline"])
def pipeline_health():
    """Row counts and ETL watermarks for all tables."""
    counts = execute("""
        SELECT 'fact_orders'         AS table_name, COUNT(*) AS rows FROM fact_orders
        UNION ALL SELECT 'fact_payments',         COUNT(*) FROM fact_payments
        UNION ALL SELECT 'fact_product_clicks',   COUNT(*) FROM fact_product_clicks
        UNION ALL SELECT 'dim_customer',          COUNT(*) FROM dim_customer
        UNION ALL SELECT 'dim_product',           COUNT(*) FROM dim_product
    """)
    watermarks = execute("""
        SELECT table_name, last_loaded_at, rows_loaded
        FROM etl_watermarks
    """)
    return {"table_counts": counts, "watermarks": watermarks}


@app.get("/pipeline/runs", tags=["Pipeline"])
def pipeline_runs(limit: int = Query(default=20, ge=1, le=100)):
    """Recent pipeline run log."""
    return execute("""
        SELECT job_name, started_at, finished_at, status, rows_processed, error_message
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT %(limit)s
    """, {"limit": limit})