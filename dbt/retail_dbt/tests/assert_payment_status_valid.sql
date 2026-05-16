-- Custom test: payment status must be one of three valid values
-- Catches any upstream data quality issues before they hit the marts

SELECT
    payment_id,
    payment_status
FROM {{ ref('stg_payments') }}
WHERE payment_status NOT IN ('success', 'failed', 'pending')