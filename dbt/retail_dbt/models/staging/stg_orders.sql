-- Staging layer: clean and cast fact_orders
-- One row per order, standardised column names and types

SELECT
    order_id,
    customer_id,
    order_date,
    CAST(total_amount AS NUMERIC(12,2))     AS total_amount,
    item_count,
    LOWER(status)                           AS status,
    LOWER(device)                           AS device,
    UPPER(region)                           AS region,
    currency,
    created_at

FROM {{ source('warehouse', 'fact_orders') }}

WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND total_amount > 0