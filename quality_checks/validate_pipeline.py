"""
validate_pipeline.py

Data quality checks for the modern-data-stack-pipeline.
Validates row counts, null rates, duplicates, and revenue totals
across the Snowflake medallion layers.

Each check returns a dict:
    {
        "check_name": str,
        "status":     "pass" | "fail",
        "details":    str
    }

run_all_checks() runs every check and returns the full list.
Called by the Airflow DAG (Step 7) and can also be run standalone.
"""

import logging
import os

import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("validate_pipeline")


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
NULL_RATE_THRESHOLD    = 0.01   # alert if >1% nulls in a critical column
REVENUE_MIN            = 1_000  # minimum expected total revenue in gold layer
REVENUE_MAX            = 100_000_000  # maximum expected total revenue


def _get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="ANALYTICS_DB",
        schema="MONITORING",
        warehouse="TRANSFORM_WH",
        role="TRANSFORMER_ROLE",
    )


def _query(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Check 1: Row count — raw vs bronze clean
# Raw row count should be close to bronze clean + rejected combined.
# ---------------------------------------------------------------------------
def check_row_counts(conn) -> dict:
    check_name = "row_count_raw_vs_bronze"
    try:
        raw_count = _query(conn, "SELECT COUNT(*) FROM RAW_DB.RAW.YELLOW_TAXI_TRIPS_RAW")[0][0]
        clean_count = _query(conn, "SELECT COUNT(*) FROM TRANSFORM_DB.BRONZE.STG_TAXI_TRIPS_CLEAN")[0][0]
        rejected_count = _query(conn, "SELECT COUNT(*) FROM TRANSFORM_DB.BRONZE.STG_TAXI_TRIPS_REJECTED")[0][0]
        bronze_total = clean_count + rejected_count

        diff = abs(raw_count - bronze_total)
        details = (
            f"raw={raw_count:,}, bronze_clean={clean_count:,}, "
            f"bronze_rejected={rejected_count:,}, diff={diff:,}"
        )

        if diff > 0:
            status = "fail"
            details = f"Row count mismatch — {details}"
        else:
            status = "pass"

        logger.info("[%s] %s — %s", status.upper(), check_name, details)
        return {"check_name": check_name, "status": status, "details": details}

    except Exception as e:
        return {"check_name": check_name, "status": "fail", "details": str(e)}


# ---------------------------------------------------------------------------
# Check 2: Null rate — critical columns in silver
# ---------------------------------------------------------------------------
def check_null_rates(conn) -> dict:
    check_name = "null_rate_silver"
    critical_columns = ["trip_id", "pickup_datetime", "dropoff_datetime", "trip_duration_minutes"]
    failures = []

    try:
        total = _query(conn, "SELECT COUNT(*) FROM TRANSFORM_DB.SILVER.INT_TRIPS_ENRICHED")[0][0]
        if total == 0:
            return {"check_name": check_name, "status": "fail", "details": "Silver table is empty"}

        for col in critical_columns:
            null_count = _query(
                conn,
                f"SELECT COUNT(*) FROM TRANSFORM_DB.SILVER.INT_TRIPS_ENRICHED WHERE {col} IS NULL"
            )[0][0]
            null_rate = null_count / total
            if null_rate > NULL_RATE_THRESHOLD:
                failures.append(f"{col}: {null_rate:.2%} nulls ({null_count:,} rows)")

        if failures:
            details = "Null rate exceeded threshold — " + "; ".join(failures)
            status = "fail"
        else:
            details = f"All {len(critical_columns)} columns within null threshold ({NULL_RATE_THRESHOLD:.0%})"
            status = "pass"

        logger.info("[%s] %s — %s", status.upper(), check_name, details)
        return {"check_name": check_name, "status": status, "details": details}

    except Exception as e:
        return {"check_name": check_name, "status": "fail", "details": str(e)}


# ---------------------------------------------------------------------------
# Check 3: Duplicate trip IDs in silver
# ---------------------------------------------------------------------------
def check_no_duplicates(conn) -> dict:
    check_name = "no_duplicate_trip_ids"
    try:
        result = _query(conn, """
            SELECT COUNT(*) FROM (
                SELECT trip_id, COUNT(*) AS cnt
                FROM TRANSFORM_DB.SILVER.INT_TRIPS_ENRICHED
                GROUP BY trip_id
                HAVING cnt > 1
            )
        """)
        duplicate_count = result[0][0]

        if duplicate_count > 0:
            status = "fail"
            details = f"{duplicate_count:,} duplicate trip_id values found in silver"
        else:
            status = "pass"
            details = "No duplicate trip_ids found"

        logger.info("[%s] %s — %s", status.upper(), check_name, details)
        return {"check_name": check_name, "status": status, "details": details}

    except Exception as e:
        return {"check_name": check_name, "status": "fail", "details": str(e)}


# ---------------------------------------------------------------------------
# Check 4: Revenue totals in gold within expected range
# ---------------------------------------------------------------------------
def check_revenue_range(conn) -> dict:
    check_name = "revenue_range_gold"
    try:
        result = _query(conn, "SELECT SUM(total_revenue) FROM TRANSFORM_DB.GOLD.MART_DAILY_REVENUE")
        total_revenue = result[0][0] or 0

        if total_revenue < REVENUE_MIN:
            status = "fail"
            details = f"Total revenue ${total_revenue:,.2f} is below minimum ${REVENUE_MIN:,.2f}"
        elif total_revenue > REVENUE_MAX:
            status = "fail"
            details = f"Total revenue ${total_revenue:,.2f} exceeds maximum ${REVENUE_MAX:,.2f}"
        else:
            status = "pass"
            details = f"Total revenue ${total_revenue:,.2f} is within expected range"

        logger.info("[%s] %s — %s", status.upper(), check_name, details)
        return {"check_name": check_name, "status": status, "details": details}

    except Exception as e:
        return {"check_name": check_name, "status": "fail", "details": str(e)}


# ---------------------------------------------------------------------------
# Check 5: Silver row count >= bronze clean row count
# Enrichment should not drop rows.
# ---------------------------------------------------------------------------
def check_silver_completeness(conn) -> dict:
    check_name = "silver_completeness"
    try:
        clean_count = _query(conn, "SELECT COUNT(*) FROM TRANSFORM_DB.BRONZE.STG_TAXI_TRIPS_CLEAN")[0][0]
        silver_count = _query(conn, "SELECT COUNT(*) FROM TRANSFORM_DB.SILVER.INT_TRIPS_ENRICHED")[0][0]

        if silver_count < clean_count:
            status = "fail"
            details = f"Silver ({silver_count:,}) has fewer rows than bronze clean ({clean_count:,})"
        else:
            status = "pass"
            details = f"Silver ({silver_count:,}) matches bronze clean ({clean_count:,})"

        logger.info("[%s] %s — %s", status.upper(), check_name, details)
        return {"check_name": check_name, "status": status, "details": details}

    except Exception as e:
        return {"check_name": check_name, "status": "fail", "details": str(e)}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_all_checks() -> list:
    logger.info("Starting pipeline quality checks...")
    conn = _get_connection()
    results = []

    try:
        checks = [
            check_row_counts,
            check_null_rates,
            check_no_duplicates,
            check_revenue_range,
            check_silver_completeness,
        ]
        for check_fn in checks:
            results.append(check_fn(conn))
    finally:
        conn.close()

    passed = sum(1 for r in results if r["status"] == "pass")
    failed = sum(1 for r in results if r["status"] == "fail")
    logger.info("Quality checks complete — %d passed, %d failed", passed, failed)

    return results


if __name__ == "__main__":
    results = run_all_checks()
    print("\n=== Quality Check Results ===")
    for r in results:
        icon = "✓" if r["status"] == "pass" else "✗"
        print(f"  {icon} [{r['status'].upper()}] {r['check_name']}: {r['details']}")
