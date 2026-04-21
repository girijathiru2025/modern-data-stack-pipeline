{{
    config(
        materialized='table'
    )
}}

SELECT 
    trip_id,
        vendor_id::INTEGER as vendor_id, 
        TO_TIMESTAMP_NTZ(pickup_datetime, 9) as pickup_datetime, -- The 9 tells Snowflake the number is in nanoseconds.
        TO_TIMESTAMP_NTZ(dropoff_datetime, 9) as dropoff_datetime,
        passenger_count::INTEGER as passenger_count,
        trip_distance::DECIMAL(10,4) as trip_distance,
        rate_code_id::INTEGER as rate_code_id,
        store_and_fwd_flag,
        pu_location_id::INTEGER as pu_location_id,
        do_location_id::INTEGER as do_location_id,
        payment_type::INTEGER as payment_type,
        fare_amount::DECIMAL(10,2) as fare_amount,
        extra::DECIMAL(10,2) as extra,
        mta_tax::DECIMAL(10,2) as mta_tax,
        tip_amount::DECIMAL(10,2) as tip_amount,
        tolls_amount::DECIMAL(10,2) as tolls_amount,
        improvement_surcharge::DECIMAL(10,2) as improvement_surcharge,
        total_amount::DECIMAL(10,2) as total_amount,
        congestion_surcharge::DECIMAL(10,2) as congestion_surcharge,
        airport_fee::DECIMAL(10,2) as airport_fee 
FROM 
    {{ ref('stg_taxi_trips') }}
WHERE 
    reason_flag = 'Valid'