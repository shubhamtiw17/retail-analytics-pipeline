-- ── Streaming aggregation tables (written by PySpark) ────────────────────────

CREATE TABLE IF NOT EXISTS agg_orders_per_minute (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    region            TEXT,
    order_count       BIGINT,
    revenue           NUMERIC(12,2),
    avg_order_value   NUMERIC(10,2),
    computed_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agg_top_products (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    product_id        TEXT,
    category          TEXT,
    click_count       BIGINT,
    avg_duration_ms   NUMERIC(10,0),
    unique_visitors   BIGINT,
    computed_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ── Pipeline run log (written by every job for lineage tracking) ─────────────

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL PRIMARY KEY,
    job_name        TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'running',
    rows_processed  BIGINT      DEFAULT 0,
    error_message   TEXT
);

-- ── Indexes for dashboard query performance ───────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_orders_window_start
    ON agg_orders_per_minute (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_products_window_start
    ON agg_top_products (window_start DESC, click_count DESC);