{{
    config(
        materialized='table'
    )
}}

SELECT 
    *
FROM 
    {{ ref('stg_taxi_trips') }}
WHERE 
    reason_flag != 'Valid'