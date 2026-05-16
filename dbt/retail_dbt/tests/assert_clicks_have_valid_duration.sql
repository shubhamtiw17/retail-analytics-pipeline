-- Custom test: session duration must be positive and realistic
-- Cap at 2 hours (7200 seconds) — anything above is a tracking bug

SELECT
    click_id,
    duration_seconds
FROM {{ ref('stg_clicks') }}
WHERE duration_seconds <= 0
   OR duration_seconds > 7200