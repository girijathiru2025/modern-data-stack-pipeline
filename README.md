# modern-data-stack-pipeline

A production-style data engineering snowflake project demonstrating **Medallion Architecture** (Bronze / Silver / Gold).

---
## Architecture
```
Raw Data (Parquet)
        ↓
Python Ingestion Script
(Snowflake Python connector)
        ↓
Snowflake Raw Layer
(Bronze — raw tables)
        ↓
dbt Models
(Silver — cleaned/standardized)
(Gold — analytical/aggregated)
        ↓
Airflow DAG
(Orchestrates end-to-end pipeline)


```
---