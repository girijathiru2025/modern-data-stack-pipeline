{{
    config(
        materialized='table'
    )
}}

SELECT
    payment_type,
    COUNT(*)                                AS total_trips,
    ROUND(AVG(trip_duration_minutes), 2)    AS avg_duration_minutes,
    ROUND(AVG(cost_per_mile), 2)            AS avg_cost_per_mile,
    ROUND(AVG(total_amount), 2)             AS avg_total_amount,
    ROUND(AVG(tip_amount), 2)               AS avg_tip_amount,
    ROUND(SUM(total_amount), 2)             AS total_revenue
FROM {{ ref('int_trips_enriched') }}
GROUP BY 1
ORDER BY 1
