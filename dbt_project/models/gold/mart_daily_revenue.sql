{{
    config(
        materialized='table'
    )
}}

SELECT
    DATE(pickup_datetime)           AS trip_date,
    pu_location_id,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(trip_distance), 4)    AS avg_trip_distance,
    ROUND(AVG(fare_amount), 2)      AS avg_fare_amount
FROM {{ ref('int_trips_enriched') }}
GROUP BY 1, 2
ORDER BY 1, 2
