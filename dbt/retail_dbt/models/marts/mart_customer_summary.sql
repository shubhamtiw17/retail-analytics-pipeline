-- Customer-level KPIs
-- Business question: Who are our best customers?

SELECT
    o.customer_id,
    dc.preferred_device,
    dc.first_seen_at::DATE                  AS first_order_date,
    dc.last_seen_at::DATE                   AS last_order_date,

    -- Order metrics
    COUNT(DISTINCT o.order_id)              AS total_orders,
    SUM(o.total_amount)                     AS lifetime_value,
    AVG(o.total_amount)                     AS avg_order_value,
    MAX(o.total_amount)                     AS largest_order,

    -- Payment behaviour
    COUNT(DISTINCT CASE
        WHEN p.payment_status = 'success'
        THEN p.payment_id END)              AS successful_payments,
    COUNT(DISTINCT CASE
        WHEN p.payment_status = 'failed'
        THEN p.payment_id END)              AS failed_payments,

    -- Engagement
    COUNT(DISTINCT c.click_id)              AS total_clicks,
    COUNT(DISTINCT c.product_id)            AS unique_products_viewed,

    -- Customer segment
    CASE
        WHEN SUM(o.total_amount) >= 10000   THEN 'platinum'
        WHEN SUM(o.total_amount) >= 5000    THEN 'gold'
        WHEN SUM(o.total_amount) >= 1000    THEN 'silver'
        ELSE                                     'bronze'
    END                                     AS customer_segment

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ source('warehouse', 'dim_customer') }} dc
    ON o.customer_id = dc.customer_id
LEFT JOIN {{ ref('stg_payments') }} p
    ON o.customer_id = p.customer_id
LEFT JOIN {{ ref('stg_clicks') }} c
    ON o.customer_id = c.customer_id

GROUP BY o.customer_id, dc.preferred_device, dc.first_seen_at, dc.last_seen_at
ORDER BY lifetime_value DESC