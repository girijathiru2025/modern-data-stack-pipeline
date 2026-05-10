{{
    config(
        materialized='table'
    )
}}

SELECT
    trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,

    -- Derived: trip duration in minutes
    DATEDIFF('minute', pickup_datetime, dropoff_datetime)           AS trip_duration_minutes,

    -- Derived: cost per mile (guard against zero-distance trips)
    CASE
        WHEN trip_distance > 0 THEN ROUND(total_amount / trip_distance, 2)
        ELSE NULL
    END                                                             AS cost_per_mile,

    -- Derived: bucket pickup hour into time-of-day segments
    CASE
        WHEN HOUR(pickup_datetime) BETWEEN 6  AND 11 THEN 'morning'
        WHEN HOUR(pickup_datetime) BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN HOUR(pickup_datetime) BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'night'
    END                                                             AS time_of_day_bucket,

    -- Derived: weekend flag (Snowflake DAYOFWEEK: 0=Sun, 6=Sat)
    CASE
        WHEN DAYOFWEEK(pickup_datetime) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                                             AS is_weekend

FROM {{ ref('stg_taxi_trips_clean') }}
