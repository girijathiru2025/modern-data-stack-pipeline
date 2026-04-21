{{
    config(
        materialized='table'
    )
}}

/*  Normally Snowflake column names are case-insensitive.  
    But when a column is created with double quotes (e.g., "VendorID", "tpep_pickup_datetime"), 
    Snowflake preserves the exact case and so you must always reference it with double quotes.
*/
-- generate_surrogate_key is upper casing the mixed case fields eg. VENDORID

-- This snowflake RAW table has 4 fields in mixed case, and all fields are created with double quotes.
-- dbt_convention - snake_case column names

-- Creating a CTE for aliasing columns as unquoted identifiers,  e.g., "VendorID" as vendor_id
-- In Snowflake, unquoted identifiers are always stored as uppercase in destiantion table, 
-- and when queried Snowflake automatically uppercases  the column name

WITH t_source AS (
    SELECT 
        "VendorID" as vendor_id,                                                                                                                                                
        "tpep_pickup_datetime" as pickup_datetime,
        "tpep_dropoff_datetime" as dropoff_datetime,
        "passenger_count" as passenger_count,                                                                                                                                           
        "trip_distance" as trip_distance,
        "RatecodeID" as rate_code_id,  
        "store_and_fwd_flag" as store_and_fwd_flag,
        "PULocationID" as pu_location_id,                                                                                                                                               
        "DOLocationID" as do_location_id,             
        "payment_type" as payment_type,
        "fare_amount" as fare_amount,
        "extra" as extra,
        "mta_tax" as mta_tax,
        "tip_amount" as tip_amount,
        "tolls_amount" as tolls_amount,
        "improvement_surcharge" as improvement_surcharge,
        "total_amount" as total_amount,
        "congestion_surcharge" as congestion_surcharge,
        "airport_fee" as airport_fee
FROM 
    RAW_DB.RAW.YELLOW_TAXI_TRIPS_RAW 
)

-- dbt adds semi colon automatically.
SELECT 
-- adding surrogate key 
{{ dbt_utils.generate_surrogate_key( ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'total_amount', 'pu_location_id', 'do_location_id', 'store_and_fwd_flag']) }} as trip_id 
    , * 
    , CASE 
        WHEN pickup_datetime IS NULL THEN 'Invalid pickup_datetime'
        WHEN dropoff_datetime IS NULL THEN 'Invalid dropoff_datetime' 
        WHEN trip_distance IS NULL OR trip_distance <= 0 THEN 'Invalid trip_distance'
        ELSE 'Valid'
    END as reason_flag

FROM 
    t_source
