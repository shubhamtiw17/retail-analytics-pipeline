-- Custom test: every order must have positive revenue
-- Fails if any rows returned

SELECT
    order_id,
    total_amount
FROM {{ ref('stg_orders') }}
WHERE total_amount <= 0