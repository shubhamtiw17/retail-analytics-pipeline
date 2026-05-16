-- Staging layer: clean and cast fact_product_clicks

SELECT
    click_id,
    customer_id,
    product_id,
    click_date,
    session_id,
    LOWER(device)                           AS device,
    LOWER(referrer)                         AS referrer,
    duration_ms,
    CAST(duration_ms / 1000.0 AS NUMERIC(10,2)) AS duration_seconds,
    created_at

FROM {{ source('warehouse', 'fact_product_clicks') }}

WHERE click_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND duration_ms > 0