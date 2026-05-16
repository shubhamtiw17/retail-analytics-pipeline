-- Product performance KPIs
-- Business question: Which products get the most attention and convert best?

SELECT
    c.product_id,
    dp.category,
    c.click_date,

    COUNT(DISTINCT c.click_id)              AS total_clicks,
    COUNT(DISTINCT c.customer_id)           AS unique_visitors,
    AVG(c.duration_seconds)                 AS avg_session_duration_sec,
    COUNT(DISTINCT c.session_id)            AS unique_sessions,

    -- Referrer breakdown
    COUNT(DISTINCT CASE WHEN c.referrer = 'search'   THEN c.click_id END) AS clicks_from_search,
    COUNT(DISTINCT CASE WHEN c.referrer = 'social'   THEN c.click_id END) AS clicks_from_social,
    COUNT(DISTINCT CASE WHEN c.referrer = 'email'    THEN c.click_id END) AS clicks_from_email,
    COUNT(DISTINCT CASE WHEN c.referrer = 'direct'   THEN c.click_id END) AS clicks_direct,

    -- Device breakdown
    COUNT(DISTINCT CASE WHEN c.device = 'mobile'  THEN c.click_id END)    AS mobile_clicks,
    COUNT(DISTINCT CASE WHEN c.device = 'desktop' THEN c.click_id END)    AS desktop_clicks,
    COUNT(DISTINCT CASE WHEN c.device = 'tablet'  THEN c.click_id END)    AS tablet_clicks

FROM {{ ref('stg_clicks') }} c
LEFT JOIN {{ source('warehouse', 'dim_product') }} dp
    ON c.product_id = dp.product_id

GROUP BY c.product_id, dp.category, c.click_date
ORDER BY c.click_date DESC, total_clicks DESC