-- ============================================================
-- STREAMS & TASKS: Incremental processing on raw taxi trips
--
-- How it works:
--   1. A Stream sits on top of the raw table and tracks every
--      INSERT since the last time it was consumed.
--   2. A Task runs on a schedule, reads new rows from the stream,
--      applies the same cleaning logic as the bronze dbt models,
--      and merges them into a processed_trips table.
--   3. The stream offset advances automatically once the Task DML
--      commits — so each row is processed exactly once.
--
-- Run this script as SYSADMIN (or a role with OWNERSHIP on these
-- objects). TRANSFORMER_ROLE needs additional grants added below.
-- ============================================================


-- ============================================================
-- STEP 1: TARGET TABLE
-- Mirrors the cleaned shape produced by stg_taxi_trips_clean,
-- plus the four derived fields from int_trips_enriched.
-- Lives in TRANSFORM_DB so TRANSFORMER_ROLE already has access.
-- ============================================================

CREATE TABLE IF NOT EXISTS TRANSFORM_DB.BRONZE.PROCESSED_TRIPS (
    trip_id                 VARCHAR(32)     NOT NULL,
    vendor_id               INTEGER,
    pickup_datetime         TIMESTAMP_NTZ,
    dropoff_datetime        TIMESTAMP_NTZ,
    passenger_count         INTEGER,
    trip_distance           DECIMAL(10, 4),
    rate_code_id            INTEGER,
    store_and_fwd_flag      VARCHAR(1),
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            INTEGER,
    fare_amount             DECIMAL(10, 2),
    extra                   DECIMAL(10, 2),
    mta_tax                 DECIMAL(10, 2),
    tip_amount              DECIMAL(10, 2),
    tolls_amount            DECIMAL(10, 2),
    improvement_surcharge   DECIMAL(10, 2),
    total_amount            DECIMAL(10, 2),
    congestion_surcharge    DECIMAL(10, 2),
    airport_fee             DECIMAL(10, 2),
    -- derived fields
    trip_duration_minutes   INTEGER,
    cost_per_mile           DECIMAL(10, 2),
    time_of_day_bucket      VARCHAR(20),
    is_weekend              BOOLEAN,
    -- audit
    _processed_at           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_processed_trips PRIMARY KEY (trip_id)
);


-- ============================================================
-- STEP 2: STREAM
-- APPEND_ONLY = TRUE because the raw table only receives INSERTs.
-- This is more efficient than a standard stream — it skips
-- update/delete tracking overhead entirely.
-- ============================================================

CREATE STREAM IF NOT EXISTS RAW_DB.RAW.TAXI_TRIPS_STREAM
    ON TABLE RAW_DB.RAW.YELLOW_TAXI_TRIPS_RAW
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new rows loaded into the raw taxi trips table';


-- ============================================================
-- STEP 3: TASK
-- Runs every hour using TRANSFORM_WH.
-- Uses MERGE so re-runs are idempotent — if the same trip_id
-- arrives twice (e.g., after a reload) it won't create a duplicate.
-- The task only fires when the stream actually has new rows
-- (WHEN SYSTEM$STREAM_HAS_DATA), saving warehouse credits on
-- quiet hours.
-- ============================================================

CREATE TASK IF NOT EXISTS TRANSFORM_DB.BRONZE.PROCESS_NEW_TRIPS
    WAREHOUSE   = TRANSFORM_WH
    SCHEDULE    = '60 MINUTE'
    COMMENT     = 'Hourly: reads new rows from stream and merges into processed_trips'
WHEN
    SYSTEM$STREAM_HAS_DATA('RAW_DB.RAW.TAXI_TRIPS_STREAM')
AS
MERGE INTO TRANSFORM_DB.BRONZE.PROCESSED_TRIPS AS tgt
USING (
    SELECT
        MD5(
            COALESCE(CAST("tpep_pickup_datetime"  AS VARCHAR), '') ||
            COALESCE(CAST("tpep_dropoff_datetime" AS VARCHAR), '') ||
            COALESCE(CAST("PULocationID"          AS VARCHAR), '') ||
            COALESCE(CAST("DOLocationID"          AS VARCHAR), '') ||
            COALESCE(CAST("fare_amount"           AS VARCHAR), '')
        )                                                           AS trip_id,
        "VendorID"::INTEGER                                         AS vendor_id,
        TO_TIMESTAMP_NTZ("tpep_pickup_datetime",  9)                AS pickup_datetime,
        TO_TIMESTAMP_NTZ("tpep_dropoff_datetime", 9)                AS dropoff_datetime,
        "passenger_count"::INTEGER                                  AS passenger_count,
        "trip_distance"::DECIMAL(10, 4)                             AS trip_distance,
        "RatecodeID"::INTEGER                                       AS rate_code_id,
        "store_and_fwd_flag"                                                AS store_and_fwd_flag,
        "PULocationID"::INTEGER                                     AS pu_location_id,
        "DOLocationID"::INTEGER                                     AS do_location_id,
        "payment_type"::INTEGER                                     AS payment_type,
        "fare_amount"::DECIMAL(10, 2)                               AS fare_amount,
        "extra"::DECIMAL(10, 2)                                     AS extra,
        "mta_tax"::DECIMAL(10, 2)                                   AS mta_tax,
        "tip_amount"::DECIMAL(10, 2)                                AS tip_amount,
        "tolls_amount"::DECIMAL(10, 2)                              AS tolls_amount,
        "improvement_surcharge"::DECIMAL(10, 2)                     AS improvement_surcharge,
        "total_amount"::DECIMAL(10, 2)                              AS total_amount,
        "congestion_surcharge"::DECIMAL(10, 2)                      AS congestion_surcharge,
        "airport_fee"::DECIMAL(10, 2)                               AS airport_fee,
        -- derived fields
        DATEDIFF('minute',
            TO_TIMESTAMP_NTZ("tpep_pickup_datetime", 9),
            TO_TIMESTAMP_NTZ("tpep_dropoff_datetime", 9))           AS trip_duration_minutes,
        CASE
            WHEN "trip_distance" > 0
            THEN ROUND("total_amount"::DECIMAL(10,2) / "trip_distance"::DECIMAL(10,4), 2)
            ELSE NULL
        END                                                         AS cost_per_mile,
        CASE
            WHEN HOUR(TO_TIMESTAMP_NTZ("tpep_pickup_datetime", 9)) BETWEEN 6  AND 11 THEN 'morning'
            WHEN HOUR(TO_TIMESTAMP_NTZ("tpep_pickup_datetime", 9)) BETWEEN 12 AND 16 THEN 'afternoon'
            WHEN HOUR(TO_TIMESTAMP_NTZ("tpep_pickup_datetime", 9)) BETWEEN 17 AND 20 THEN 'evening'
            ELSE 'night'
        END                                                         AS time_of_day_bucket,
        CASE
            WHEN DAYOFWEEK(TO_TIMESTAMP_NTZ("tpep_pickup_datetime", 9)) IN (0, 6) THEN TRUE
            ELSE FALSE
        END                                                         AS is_weekend
    FROM RAW_DB.RAW.TAXI_TRIPS_STREAM
    WHERE "tpep_pickup_datetime"  IS NOT NULL
      AND "tpep_dropoff_datetime" IS NOT NULL
      AND "trip_distance" > 0
) AS src
ON tgt.trip_id = src.trip_id
WHEN NOT MATCHED THEN INSERT (
    trip_id, vendor_id, pickup_datetime, dropoff_datetime,
    passenger_count, trip_distance, rate_code_id, store_and_fwd_flag,
    pu_location_id, do_location_id, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    trip_duration_minutes, cost_per_mile, time_of_day_bucket, is_weekend
)
VALUES (
    src.trip_id, src.vendor_id, src.pickup_datetime, src.dropoff_datetime,
    src.passenger_count, src.trip_distance, src.rate_code_id, src.store_and_fwd_flag,
    src.pu_location_id, src.do_location_id, src.payment_type,
    src.fare_amount, src.extra, src.mta_tax, src.tip_amount, src.tolls_amount,
    src.improvement_surcharge, src.total_amount, src.congestion_surcharge, src.airport_fee,
    src.trip_duration_minutes, src.cost_per_mile, src.time_of_day_bucket, src.is_weekend
);


-- ============================================================
-- STEP 4: GRANTS
-- TRANSFORMER_ROLE needs ownership/usage on the task and
-- read access on the stream.
-- ============================================================

GRANT OWNERSHIP ON TASK   TRANSFORM_DB.BRONZE.PROCESS_NEW_TRIPS  TO ROLE TRANSFORMER_ROLE;
GRANT SELECT   ON STREAM  RAW_DB.RAW.TAXI_TRIPS_STREAM            TO ROLE TRANSFORMER_ROLE;
GRANT INSERT, UPDATE, DELETE ON TABLE TRANSFORM_DB.BRONZE.PROCESSED_TRIPS  TO ROLE TRANSFORMER_ROLE;


-- ============================================================
-- STEP 5: ACTIVATE THE TASK
-- Tasks are created in SUSPENDED state by default.
-- Resume it once you have verified the stream and target table
-- are set up correctly.
-- ============================================================

ALTER TASK TRANSFORM_DB.BRONZE.PROCESS_NEW_TRIPS RESUME;

-- To pause the task (e.g., during a maintenance window):
--   ALTER TASK TRANSFORM_DB.BRONZE.PROCESS_NEW_TRIPS SUSPEND;

-- To run it manually on demand (useful for testing):
--   EXECUTE TASK TRANSFORM_DB.BRONZE.PROCESS_NEW_TRIPS;

-- To check task run history:
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--       TASK_NAME => 'PROCESS_NEW_TRIPS'
--   )) ORDER BY SCHEDULED_TIME DESC;

-- To check stream lag (how many rows are pending):
--   SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('RAW_DB.RAW.TAXI_TRIPS_STREAM');
