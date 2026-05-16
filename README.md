# Real-Time Retail Analytics Pipeline

A production-grade streaming analytics platform that ingests e-commerce
events in real time, processes them through a lakehouse architecture, and
serves live KPI dashboards, built to mirror how platforms like Amazon
and Shopify handle operational data at scale.

---

## Architecture

```
Event Generator (Python · 50 events/sec · 5 event types)
        ↓
Apache Kafka (6 topics · 3 partitions each)
        ↓  ←──────────────── Dead Letter Queue (invalid events)
PySpark Structured Streaming (micro-batch · 30s trigger)
        ↓
MinIO S3 Data Lake (Parquet · partitioned by date · Bronze layer)
        ↓
Apache Airflow DAG (daily · incremental load · watermark tracking)
        ↓
PostgreSQL Warehouse (star schema · fact + dim tables)
        ↓
dbt Transformations (staging views · mart tables · 19 tests)
        ↓
Streamlit Dashboard + FastAPI (4 tabs · 8 REST endpoints)
```

---

## Tech Stack

| Layer         | Technology                          |
|---------------|-------------------------------------|
| Ingestion     | Python · Apache Kafka               |
| Streaming     | PySpark Structured Streaming        |
| Storage       | MinIO (S3-compatible) · Parquet     |
| Orchestration | Apache Airflow                      |
| Warehouse     | PostgreSQL · star schema            |
| Transform     | dbt-core                            |
| Quality       | dbt tests · custom SQL assertions   |
| Serving       | Streamlit · FastAPI · Plotly        |
| Infra         | Docker Compose · GitHub Actions     |

---

## Performance

| Metric | Value |
|---|---|
| Generator throughput | 50 events/sec (500/sec flash sale) |
| End-to-end latency | ~38 seconds event → MinIO |
| Spark batch time | ~8 seconds per topic |
| dbt run time | 1.5 seconds (6 models) |
| dbt test time | 1.75 seconds (19 tests) |
| Dashboard refresh | 30 seconds |

---

## Quick Start

**Prerequisites:** Docker Desktop, Python 3.10+, Git

```bash
# 1. Clone
git clone https://github.com/shubhamtiw17/retail-analytics-pipeline
cd retail-analytics-pipeline

# 2. Environment
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Install Python deps
pip install -r requirements.txt

# 5. Run event generator
python generators/event_generator.py --broker localhost:9092 --tps 50

# 6. Run streaming ingest (new terminal — Windows: set HADOOP_HOME=C:\hadoop first)
python spark/jobs/stream_ingest.py
```

---

## Service URLs

| Service        | URL                        | Credentials         |
|----------------|----------------------------|---------------------|
| Kafka UI       | http://localhost:8085      | —                   |
| MinIO Console  | http://localhost:9001      | admin / password123 |
| Spark Master   | http://localhost:8081      | —                   |
| Airflow        | http://localhost:8080      | admin / admin       |
| Dashboard      | http://localhost:8501      | —                   |
| API Swagger    | http://localhost:8000/docs | —                   |

---

## Event Types

| Topic              | Description                      | Volume |
|--------------------|----------------------------------|--------|
| orders             | Orders with line items           | 1x     |
| payments           | Payment attempts and outcomes    | 1x     |
| customer_events    | Signups, logins, profile updates | 2x     |
| inventory_updates  | Stock level changes              | 1x     |
| product_clicks     | Browsing and clickstream         | 10x    |

---

## Data Model

```
fact_orders ──────────┐
fact_payments          ├──── dim_customer
fact_product_clicks ──┘      dim_product
                              dim_date
```

**Mart tables (dbt):**
- `mart_daily_revenue` — revenue KPIs by date and region
- `mart_product_performance` — click metrics by product and channel
- `mart_customer_summary` — LTV and segmentation (platinum/gold/silver/bronze)

---

## dbt Tests

19 tests covering:
- `not_null` and `unique` on all primary keys
- `accepted_values` on status and segment columns
- Custom SQL assertions: positive revenue, valid payment status,
  realistic session duration

---

## Design Decisions

See [docs/DECISIONS.md](./docs/DECISIONS.md) for 12 documented architecture
decisions covering tool choices, trade-offs, and production considerations.

---

## Operations

See [docs/RUNBOOK.md](./docs/RUNBOOK.md) for:
- Full stack startup procedure
- 6 failure scenarios with diagnosis and recovery steps
- Performance benchmark table
- Healthcheck commands

---

## Project Status

- [x] Phase 1 — Infrastructure, Kafka, event generator
- [x] Phase 2 — PySpark structured streaming, MinIO bronze layer
- [x] Phase 3 — Star schema, Airflow ETL DAG
- [x] Phase 4 — dbt models, 19 tests, lineage docs
- [x] Phase 5 — Streamlit dashboard, FastAPI
- [x] Phase 6 — DECISIONS.md, RUNBOOK.md, benchmarks, CI

---

## Screenshots

### Kafka — 6 topics with live message flow
![Kafka Topics](screenshots/kafka-topics.png)

### MinIO — Bronze layer Parquet files partitioned by date
![MinIO Bronze](screenshots/minio-bronze.png)

### Airflow — ETL DAG all green
![Airflow DAG](screenshots/dag-pipeline.png)

### Dashboard — Live KPIs
![Dashboard KPIs](screenshots/images-3.png)
![Dashboard KPIs](screenshots/images-4.png)

### Dashboard — Product Performance
![Dashboard Products](screenshots/images-5.png)

### Dashboard — Pipeline Health
![Pipeline Health](screenshots/images-6.png)

### dbt — Lineage Graph
![dbt Lineage](screenshots/image-2.png)

---