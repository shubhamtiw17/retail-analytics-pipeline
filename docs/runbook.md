# Operations Runbook

Practical guide for operating and recovering the retail analytics pipeline.

---

## Starting the full stack

```bash
# 1. Start all Docker services
docker-compose up -d

# 2. Verify all services healthy
docker-compose ps

# 3. Start event generator (Terminal 1)
python generators/event_generator.py --broker localhost:9092 --tps 50

# 4. Start streaming ingest (Terminal 2)
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
python spark/jobs/stream_ingest.py

# 5. Start aggregations (Terminal 3)
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
python spark/jobs/stream_aggregations.py

# 6. Start dashboard (Terminal 4)
streamlit run dashboard/app.py

# 7. Start API (Terminal 5)
uvicorn api.main:app --reload --port 8000
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

## Scenario 1 — Kafka consumer lag spike

**Symptom:** Kafka UI shows consumer group lag growing rapidly.

**Cause:** Spark micro-batch is falling behind the producer rate.

**Fix:**
```bash
# Check current lag
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group spark-ingest

# Option 1: Reduce generator TPS temporarily
python generators/event_generator.py --broker localhost:9092 --tps 10

# Option 2: Restart ingest job (picks up from checkpoint)
# Ctrl+C the stream_ingest.py process, then restart it
$env:HADOOP_HOME = "C:\hadoop"
python spark/jobs/stream_ingest.py
```

**Prevention:** Monitor `agg_orders_per_minute` row count growth rate.
If it slows, lag is building.

---

## Scenario 2 — DLQ events accumulating

**Symptom:** Kafka UI shows messages in `dead_letter_queue` topic.

**Cause:** Events failing schema validation — malformed JSON or missing
required fields.

**Inspect:**
```bash
# Read DLQ messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dead_letter_queue \
  --from-beginning \
  --max-messages 10
```

**Replay:** Fix the schema issue in the generator, then reset the DLQ
consumer offset to re-process from the beginning.

---

## Scenario 3 — Spark checkpoint corruption

**Symptom:** `stream_ingest.py` crashes immediately on startup with
checkpoint-related errors.

**Fix:**
```bash
# Clear checkpoints (causes reprocessing from latest Kafka offset)
Remove-Item -Recurse -Force data\staging\checkpoints

# Restart ingest job
$env:HADOOP_HOME = "C:\hadoop"
python spark/jobs/stream_ingest.py
```

**Note:** Clearing checkpoints means events produced while the job was
down will not be reprocessed (startingOffsets=latest). For exactly-once
guarantees in production, use `startingOffsets=earliest` and idempotent
writes.

---

## Scenario 4 — Airflow DAG failing

**Symptom:** Tasks going red in Airflow UI.

**Diagnose:**
1. Click the failed task in Airflow UI
2. Click **Log** tab
3. Read the Python traceback

**Common causes:**
- PostgreSQL connection refused → check `docker-compose ps` for postgres health
- Parquet files not found → check `data/staging/bronze/` exists and has files
- pyarrow not installed → `docker exec airflow-webserver python -m pip install pyarrow pandas --user`

**Restart after fix:**
```bash
# In Airflow UI: DAG → failed run → Clear → Trigger
```

---

## Scenario 5 — dbt model failure

**Symptom:** `dbt run` exits with errors.

**Diagnose:**
```bash
cd dbt\retail_dbt

# Run single model to isolate the issue
dbt run --select mart_daily_revenue

# Test single model
dbt test --select mart_daily_revenue
```

**Roll back a bad model:**
```bash
# Re-run previous working version from git
git stash
dbt run
git stash pop
```

---

## Scenario 6 — MinIO disk full

**Symptom:** Spark ingest job logs show upload failures.

**Check usage:**
```bash
# In MinIO Console → Buckets → retail-lake → check size
```

**Fix:**
```bash
# In MinIO Console → Browse → bronze → select old date partition folders → Delete
# Keep at least the last 7 days
```

---

## Performance benchmarks

Measured on Dell laptop, Windows 10, 16GB RAM, Docker Desktop:

| Metric | Value |
|---|---|
| Generator throughput | 50 events/sec normal, 500/sec flash sale |
| Spark micro-batch interval | 30 seconds |
| Avg batch processing time | ~8 seconds per topic |
| End-to-end latency (event → MinIO) | ~38 seconds |
| Airflow DAG runtime (full load) | ~45 seconds |
| dbt run time (6 models) | ~1.5 seconds |
| dbt test time (19 tests) | ~1.75 seconds |
| PostgreSQL query time (mart tables) | <50ms |
| Dashboard refresh interval | 30 seconds |

---

## Healthcheck commands

```bash
# Kafka topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# PostgreSQL row counts
docker exec postgres psql -U retail -d retail_warehouse -c "
SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL SELECT 'fact_product_clicks', COUNT(*) FROM fact_product_clicks;"

# dbt tests
cd dbt\retail_dbt && dbt test
```