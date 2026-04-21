-- ============================================================
  -- STEP 1: WAREHOUSES
  -- Virtual compute in Snowflake. Each warehouse is independent.
  -- ============================================================

  CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60          -- suspend after 60s idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by ingestion scripts to load raw data';

  CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by dbt transformations and Snowflake Tasks';


  -- ============================================================
  -- STEP 2: DATABASES
  -- Separate databases enforce medallion layer boundaries.
  -- ============================================================

  CREATE DATABASE IF NOT EXISTS RAW_DB
    COMMENT = 'Raw ingested data — source of truth, never modified';

  CREATE DATABASE IF NOT EXISTS TRANSFORM_DB
    COMMENT = 'dbt Bronze and Silver models';

  CREATE DATABASE IF NOT EXISTS ANALYTICS_DB
    COMMENT = 'dbt Gold mart models — consumed by analysts';


  -- ============================================================
  -- STEP 3: SCHEMAS
  -- Logical namespaces within each database.
  -- ============================================================

  -- RAW_DB: one schema for all raw ingested tables
  CREATE SCHEMA IF NOT EXISTS RAW_DB.RAW
    COMMENT = 'Raw tables loaded by Python ingestion scripts';

  -- TRANSFORM_DB: separate schemas per medallion layer
  CREATE SCHEMA IF NOT EXISTS TRANSFORM_DB.BRONZE
    COMMENT = 'dbt staging models — cleaned, renamed, type-cast';

  CREATE SCHEMA IF NOT EXISTS TRANSFORM_DB.SILVER
    COMMENT = 'dbt intermediate models — enriched, derived fields';

  -- ANALYTICS_DB: gold layer
  CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.GOLD
    COMMENT = 'dbt mart models — aggregated, business-ready';

  -- Monitoring schema lives in ANALYTICS_DB
  CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.MONITORING
    COMMENT = 'Pipeline run metrics and data quality logs';


  -- ============================================================
  -- STEP 4: ROLES
  -- Principle of least privilege. Each role gets only what it needs.
  -- ============================================================

  CREATE ROLE IF NOT EXISTS LOADER_ROLE
    COMMENT = 'Used by Python ingestion scripts to write raw data';

  CREATE ROLE IF NOT EXISTS TRANSFORMER_ROLE
    COMMENT = 'Used by dbt to read raw data and write transformed models';

  CREATE ROLE IF NOT EXISTS ANALYST_ROLE
    COMMENT = 'Read-only access to Gold layer for analysts/BI tools';


  -- ============================================================
  -- STEP 5: WAREHOUSE GRANTS
  -- Each role gets the warehouse it needs.
  -- ============================================================

  GRANT USAGE ON WAREHOUSE LOADING_WH   TO ROLE LOADER_ROLE;
  GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE ANALYST_ROLE;


  -- ============================================================
  -- STEP 6: DATABASE + SCHEMA GRANTS
  -- ============================================================

  -- LOADER_ROLE: write access to RAW_DB only
  GRANT USAGE ON DATABASE RAW_DB TO ROLE LOADER_ROLE;
  GRANT USAGE ON SCHEMA RAW_DB.RAW TO ROLE LOADER_ROLE;
  GRANT CREATE TABLE ON SCHEMA RAW_DB.RAW TO ROLE LOADER_ROLE;
  GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA RAW_DB.RAW TO ROLE LOADER_ROLE;
  -- Future tables get the same grants automatically
  GRANT INSERT, UPDATE ON FUTURE TABLES IN SCHEMA RAW_DB.RAW TO ROLE LOADER_ROLE;

  -- TRANSFORMER_ROLE: read RAW_DB, write to TRANSFORM_DB and ANALYTICS_DB
  GRANT USAGE ON DATABASE RAW_DB TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON SCHEMA RAW_DB.RAW TO ROLE TRANSFORMER_ROLE;
  GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DB.RAW TO ROLE TRANSFORMER_ROLE;
  GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_DB.RAW TO ROLE TRANSFORMER_ROLE;

  GRANT USAGE ON DATABASE TRANSFORM_DB TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON SCHEMA TRANSFORM_DB.BRONZE TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON SCHEMA TRANSFORM_DB.SILVER TO ROLE TRANSFORMER_ROLE;
  GRANT CREATE TABLE, CREATE VIEW ON SCHEMA TRANSFORM_DB.BRONZE TO ROLE TRANSFORMER_ROLE;
  GRANT CREATE TABLE, CREATE VIEW ON SCHEMA TRANSFORM_DB.SILVER TO ROLE TRANSFORMER_ROLE;

  GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON SCHEMA ANALYTICS_DB.GOLD TO ROLE TRANSFORMER_ROLE;
  GRANT USAGE ON SCHEMA ANALYTICS_DB.MONITORING TO ROLE TRANSFORMER_ROLE;
  GRANT CREATE TABLE, CREATE VIEW ON SCHEMA ANALYTICS_DB.GOLD TO ROLE TRANSFORMER_ROLE;
  GRANT CREATE TABLE ON SCHEMA ANALYTICS_DB.MONITORING TO ROLE TRANSFORMER_ROLE;

  -- ANALYST_ROLE: read-only on Gold layer
  GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE ANALYST_ROLE;
  GRANT USAGE ON SCHEMA ANALYTICS_DB.GOLD TO ROLE ANALYST_ROLE;
  GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS_DB.GOLD TO ROLE ANALYST_ROLE;
  GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.GOLD TO ROLE ANALYST_ROLE;
  GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS_DB.GOLD TO ROLE ANALYST_ROLE;
  GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS_DB.GOLD TO ROLE ANALYST_ROLE;


  -- ============================================================
  -- STEP 7: SERVICE USERS
  -- ============================================================

  CREATE USER IF NOT EXISTS LOADER_USER
    PASSWORD = '<pwd>'
    DEFAULT_ROLE = LOADER_ROLE
    DEFAULT_WAREHOUSE = LOADING_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for Python ingestion scripts';

  CREATE USER IF NOT EXISTS TRANSFORMER_USER
    PASSWORD = '<pwd>'
    DEFAULT_ROLE = TRANSFORMER_ROLE
    DEFAULT_WAREHOUSE = TRANSFORM_WH
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for dbt';

  -- Assign roles to users
  GRANT ROLE LOADER_ROLE      TO USER LOADER_USER;
  GRANT ROLE TRANSFORMER_ROLE TO USER TRANSFORMER_USER;

  -- Grant roles to SYSADMIN so you can switch into them to debug
  GRANT ROLE LOADER_ROLE      TO ROLE SYSADMIN;
  GRANT ROLE TRANSFORMER_ROLE TO ROLE SYSADMIN;
  GRANT ROLE ANALYST_ROLE     TO ROLE SYSADMIN;
