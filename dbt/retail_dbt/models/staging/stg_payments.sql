-- Staging layer: clean and cast fact_payments

SELECT
    payment_id,
    order_id,
    customer_id,
    payment_date,
    CAST(amount AS NUMERIC(12,2))           AS amount,
    LOWER(method)                           AS payment_method,
    LOWER(status)                           AS payment_status,
    currency,
    gateway_ref,
    created_at

FROM {{ source('warehouse', 'fact_payments') }}

WHERE payment_id IS NOT NULL
  AND amount > 0
  AND status IN ('success', 'failed', 'pending')