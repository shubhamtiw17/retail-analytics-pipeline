# Architecture Decision Log

---

## 001 — MinIO over real AWS S3

**Question:** Should I use AWS S3 or a local alternative?

**Options considered:**
- Real AWS S3 - requires account, costs money, needs internet
- MinIO — S3 - compatible API, runs in Docker, free, works offline

**Decision:** MinIO locally, with the S3 client code written against the
standard boto3/minio SDK so switching to real S3 in production requires
only changing the endpoint URL and credentials.

**Trade-off:** MinIO doesn't replicate across availability zones, so it's
not production-durable. Acceptable for a local development platform.

---

## 002 — 3 partitions per Kafka topic

**Question:** How many partitions per topic?

**Options considered:**
- 1 partition — simple, no parallelism, can't demonstrate consumer groups
- 3 partitions — matches 1 Spark worker with 2 cores, allows parallel consumers
- 10+ partitions — overkill for local Docker, wastes memory

**Decision:** 3 partitions per topic. Enough to demonstrate partitioned
consumption and consumer group rebalancing without overloading a local machine.

---

## 003 — Weighted event volume (clicks 10x orders)

**Question:** Should all topics produce events at the same rate?

**Decision:** No, clickstream data is naturally 10–100x more frequent than
transactional data in real e-commerce. The generator reflects this with
weighted random selection. This makes the pipeline more realistic and forces
the streaming layer to handle mixed-volume topics correctly.

---

## 004 — Micro-batch over continuous streaming

**Question:** Should the Spark streaming job use continuous processing
or micro-batch?

**Options considered:**
- Continuous processing — sub-millisecond latency, higher CPU cost,
  more complex failure recovery
- Micro-batch (trigger every 30s) — predictable batches, efficient
  Parquet file sizes, simpler checkpointing

**Decision:** Micro-batch at 30-second intervals. The Streamlit dashboard
refreshes every 60 seconds so sub-second latency provides no UX benefit.
Micro-batching reduces S3 API calls by ~60% and produces larger, more
efficient Parquet files. Continuous processing would be warranted for
fraud detection or real-time bidding use cases.

---

## 005 — Separate ingest and aggregation jobs

**Question:** Should ingestion and aggregation run in the same Spark job?

**Decision:** Separate jobs. If the aggregation logic has a bug and needs
redeployment, the ingest job keeps running unaffected, no bronze data
is lost. Each job has its own checkpoint so they recover independently.

---

## 006 — Local staging over S3A for Spark → MinIO writes

**Question:** How should PySpark write Parquet files to MinIO?

**Options considered:**
- S3A filesystem connector, standard approach, writes directly to S3-compatible
  storage from Spark executors via hadoop-aws JAR
- Local staging + Python minio client upload, Spark writes to local disk,
  a Python uploader pushes files to MinIO after each micro-batch

**Decision:** Local staging + minio client. The S3A connector has a known
Py4J socket instability on Windows with Python 3.10 (Connection reset on
executor → driver communication during write tasks). The local-stage-then-upload
pattern is used internally by AWS Glue and is production-proven. In a cloud
deployment (Linux), S3A would be the correct choice and requires only changing
the write path — the streaming logic is identical.

**Trade-off:** Requires local disk space proportional to one micro-batch of
data (~30 seconds of events). Acceptable for this scale.

---

## 007 — Separate hostname configs for Docker vs local processes

**Question:** How to handle PostgreSQL hostname when both Docker containers
and local Python processes need to connect?

**Decision:** Docker services use the container hostname `postgres` (resolved
via the retail-net Docker network). Local processes (Spark, scripts) use
`localhost:5432` via the `.env` file. The Airflow DAG hardcodes `postgres`
since it always runs inside Docker and never locally.

---

## 008 — No foreign key constraints on fact tables

**Question:** Should fact tables enforce referential integrity via FK constraints?

**Decision:** No. FK constraints on fact tables cause load failures when
dimension tables aren't fully populated before facts arrive, a common
situation in streaming pipelines where events reference customers or products
not yet in the dim tables. Referential integrity is enforced instead by the
validate_quality task which checks for orphaned records and reports them
without blocking the load.

---

## 009 — dbt materialization strategy

**Question:** Should all dbt models be tables or views?

**Options considered:**
- All tables — fast query performance, higher storage cost, longer run time
- All views — zero storage, always fresh, slower dashboard queries
- Hybrid — staging as views, marts as tables

**Decision:** Staging and intermediate models are views (zero storage cost,
always reflect latest data). Mart models are tables (pre-aggregated, fast
dashboard queries). This is the standard dbt pattern used at Airbnb, GitLab,
and most production dbt deployments.

---

## 010 — Streamlit over Power BI / Tableau for dashboarding

**Question:** Which dashboarding tool to use?

**Options considered:**
- Power BI / Tableau — industry standard, requires license, not reproducible
  in a local Docker setup, can't be version controlled
- Streamlit — pure Python, version controlled, runs locally with one command,
  integrates directly with psycopg2 and plotly

**Decision:** Streamlit. Every reviewer can clone the repo and run the
dashboard in 30 seconds with no license required. The code is readable Python
rather than a proprietary format. In a production setting with a business
analyst audience, Power BI would be the right choice.

---

## 011 — Single PostgreSQL instance for warehouse + Airflow metadata

**Question:** Should Airflow use a separate database from the warehouse?

**Options considered:**
- Separate databases — cleaner isolation, two containers to manage
- Same instance, different schemas — simpler Docker setup, shared container

**Decision:** Same PostgreSQL instance. Airflow uses the `public` schema for
its metadata tables and our warehouse uses `public` for fact/dim tables and
`dbt_marts_marts` for mart tables. In production these would be separate
instances — this is a local development trade-off documented here so any
reviewer understands it was a deliberate choice, not an oversight.

---

## 012 — Python 3.10 + winutils for local Spark on Windows

**Question:** How to run PySpark locally on Windows without WSL?

**Decision:** Install winutils.exe (Hadoop Windows binaries) at C:\hadoop\bin
and set HADOOP_HOME before launching Spark. This is the standard approach for
running Hadoop-dependent tools on Windows without a full Linux environment.
The HADOOP_HOME variable must be set in the shell session before Python starts
since the JVM reads it at startup before any Python code executes.

**Production note:** In a Linux/cloud environment none of this is needed
Spark runs natively. The winutils dependency is Windows-only.