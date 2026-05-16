-- ── Dimension tables ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id     TEXT        PRIMARY KEY,
    first_seen_at   TIMESTAMPTZ,
    last_seen_at    TIMESTAMPTZ,
    total_orders    INT         DEFAULT 0,
    preferred_device TEXT,
    country         TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id      TEXT        PRIMARY KEY,
    category        TEXT,
    first_clicked   TIMESTAMPTZ,
    last_clicked    TIMESTAMPTZ,
    total_clicks    BIGINT      DEFAULT 0,
    avg_duration_ms NUMERIC(10,2),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         DATE        PRIMARY KEY,
    year            INT,
    month           INT,
    day             INT,
    quarter         INT,
    day_of_week     INT,
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_payment_method (
    method_id       SERIAL      PRIMARY KEY,
    method_name     TEXT        UNIQUE NOT NULL,
    method_type     TEXT
);

-- ── Fact tables ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        TEXT        PRIMARY KEY,
    customer_id     TEXT,
    order_date      DATE,
    total_amount    NUMERIC(12,2),
    item_count      INT,
    status          TEXT,
    device          TEXT,
    region          TEXT,
    currency        TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id      TEXT        PRIMARY KEY,
    order_id        TEXT,
    customer_id     TEXT,
    payment_date    DATE        REFERENCES dim_date(date_id),
    amount          NUMERIC(12,2),
    method          TEXT,
    status          TEXT,
    currency        TEXT,
    gateway_ref     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_product_clicks (
    click_id        TEXT        PRIMARY KEY,
    customer_id     TEXT,
    product_id      TEXT        REFERENCES dim_product(product_id),
    click_date      DATE        REFERENCES dim_date(date_id),
    session_id      TEXT,
    device          TEXT,
    referrer        TEXT,
    duration_ms     BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer
    ON fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date
    ON fact_orders (order_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_orders_region
    ON fact_orders (region);

CREATE INDEX IF NOT EXISTS idx_fact_payments_order
    ON fact_payments (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_payments_date
    ON fact_payments (payment_date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_payments_status
    ON fact_payments (status);

CREATE INDEX IF NOT EXISTS idx_fact_clicks_product
    ON fact_product_clicks (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_clicks_date
    ON fact_product_clicks (click_date DESC);

-- ── Watermark table (tracks incremental load progress) ────────────────────────

CREATE TABLE IF NOT EXISTS etl_watermarks (
    table_name      TEXT        PRIMARY KEY,
    last_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01',
    rows_loaded     BIGINT      DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO etl_watermarks (table_name) VALUES
    ('fact_orders'),
    ('fact_payments'),
    ('fact_product_clicks')
ON CONFLICT (table_name) DO NOTHING;