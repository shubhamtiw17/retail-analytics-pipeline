-- Daily revenue KPIs — answers: how much did we make today?
-- Business question: Which days and regions drive the most revenue?

SELECT
    o.order_date,
    o.region,
    COUNT(DISTINCT o.order_id)              AS total_orders,
    COUNT(DISTINCT o.customer_id)           AS unique_customers,
    SUM(o.total_amount)                     AS gross_revenue,
    AVG(o.total_amount)                     AS avg_order_value,
    SUM(o.item_count)                       AS total_items_sold,

    -- Payment success rate for this day/region
    COUNT(DISTINCT CASE
        WHEN p.payment_status = 'success'
        THEN p.payment_id END)              AS successful_payments,
    COUNT(DISTINCT p.payment_id)            AS total_payments,

    ROUND(
        COUNT(DISTINCT CASE
            WHEN p.payment_status = 'success'
            THEN p.payment_id END) * 100.0
        / NULLIF(COUNT(DISTINCT p.payment_id), 0),
        2
    )                                       AS payment_success_rate

FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p
    ON o.order_id = p.order_id

GROUP BY o.order_date, o.region
ORDER BY o.order_date DESC, gross_revenue DESC