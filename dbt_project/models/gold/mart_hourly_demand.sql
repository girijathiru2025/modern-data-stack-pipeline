{{
    config(
        materialized='table'
    )
}}

SELECT
    HOUR(pickup_datetime)           AS hour_of_day,
    DAYOFWEEK(pickup_datetime)      AS day_of_week,        -- 0=Sun … 6=Sat
    DAYNAME(pickup_datetime)        AS day_name,
    is_weekend,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM {{ ref('int_trips_enriched') }}
GROUP BY 1, 2, 3, 4
ORDER BY 2, 1
