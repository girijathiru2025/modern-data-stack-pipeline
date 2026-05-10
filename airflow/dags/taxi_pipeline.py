"""
taxi_pipeline.py

End-to-end orchestration for the modern-data-stack-pipeline:
  1. download_data        — pull latest NYC yellow taxi parquet file
  2. load_to_snowflake    — bulk-load parquet into RAW_DB.RAW
  3. dbt_run              — run all dbt models (bronze → silver → gold)
  4. dbt_test             — run all dbt schema + data tests
  5. quality_checks       — row counts, null rates, duplicate check
  6. log_metrics          — write run summary to ANALYTICS_DB.MONITORING
  7. alert_on_failure     — triggered automatically if any task fails

All Snowflake credentials come from environment variables:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

All paths are relative to PROJECT_ROOT (env var or auto-detected).
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Path setup — add project root to sys.path so we can import local modules
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default arguments applied to every task
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,   # replace with True + email list in production
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="taxi_pipeline",
    description="NYC Taxi end-to-end pipeline: ingest → dbt → quality checks → monitoring",
    schedule_interval="0 6 * * *",   # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taxi", "snowflake", "dbt"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Download raw parquet file from NYC Open Data
    # -----------------------------------------------------------------------
    def _download_data(**context):
        from ingestion.ingest import download_raw_file
        local_path = download_raw_file()
        # Push path to XCom so the next task can read it
        context["ti"].xcom_push(key="local_path", value=local_path)
        logger.info("Downloaded file to %s", local_path)

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=_download_data,
    )

    # -----------------------------------------------------------------------
    # Task 2: Load parquet into Snowflake RAW_DB
    # -----------------------------------------------------------------------
    def _load_to_snowflake(**context):
        from ingestion.ingest import load_raw_file
        local_path = context["ti"].xcom_pull(task_ids="download_data", key="local_path")
        load_raw_file(local_path)

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=_load_to_snowflake,
    )

    # -----------------------------------------------------------------------
    # Task 3: dbt run — build all models
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_project && "
            "/dbt-venv/bin/dbt run --profiles-dir . --target prod"
        ),
    )

    # -----------------------------------------------------------------------
    # Task 4: dbt test — validate all schema + data tests
    # -----------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_project && "
            "/dbt-venv/bin/dbt test --profiles-dir . --target prod"
        ),
    )

    # -----------------------------------------------------------------------
    # Task 5: Data quality checks
    # Calls validate_pipeline.py (built in Step 8).
    # Returns a dict of check results; pushes to XCom for metrics logging.
    # -----------------------------------------------------------------------
    def _quality_checks(**context):
        from quality_checks.validate_pipeline import run_all_checks
        results = run_all_checks()
        context["ti"].xcom_push(key="quality_results", value=results)

        failed = [r["check_name"] for r in results if r["status"] == "fail"]
        if failed:
            raise ValueError(f"Quality checks failed: {failed}")

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=_quality_checks,
    )

    # -----------------------------------------------------------------------
    # Task 6: Log pipeline metrics to ANALYTICS_DB.MONITORING
    # Runs even if quality_checks fails so we always have a run record.
    # -----------------------------------------------------------------------
    def _log_metrics(**context):
        import snowflake.connector

        ti = context["ti"]
        run_date = context["logical_date"].strftime("%Y-%m-%d")
        quality_results = ti.xcom_pull(task_ids="quality_checks", key="quality_results") or []

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database="ANALYTICS_DB",
            schema="MONITORING",
            warehouse="TRANSFORM_WH",
            role="TRANSFORMER_ROLE",
        )

        try:
            conn.cursor().execute("""
                CREATE TABLE IF NOT EXISTS ANALYTICS_DB.MONITORING.PIPELINE_METRICS (
                    run_id          VARCHAR(64),
                    run_date        DATE,
                    check_name      VARCHAR(128),
                    status          VARCHAR(16),
                    details         VARCHAR(1024),
                    logged_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)

            run_id = context["run_id"]
            rows = [
                (run_id, run_date, r["check_name"], r["status"], r.get("details", ""))
                for r in quality_results
            ]

            if rows:
                conn.cursor().executemany(
                    """
                    INSERT INTO ANALYTICS_DB.MONITORING.PIPELINE_METRICS
                        (run_id, run_date, check_name, status, details)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    rows,
                )
                logger.info("Logged %d quality check results to MONITORING", len(rows))
            else:
                # Log a generic success row when no checks produced results
                conn.cursor().execute(
                    """
                    INSERT INTO ANALYTICS_DB.MONITORING.PIPELINE_METRICS
                        (run_id, run_date, check_name, status, details)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (run_id, run_date, "pipeline_run", "pass", "No quality check results"),
                )
        finally:
            conn.close()

    log_metrics = PythonOperator(
        task_id="log_metrics",
        python_callable=_log_metrics,
        trigger_rule=TriggerRule.ALL_DONE,   # runs even if quality_checks fails
    )

    # -----------------------------------------------------------------------
    # Task 7: Alert on failure
    # Triggered only when at least one upstream task fails.
    # Logs to Airflow; swap the body for an email/Slack call in production.
    # -----------------------------------------------------------------------
    def _alert_on_failure(**context):
        failed_tasks = [
            t.task_id
            for t in context["dag_run"].get_task_instances()
            if t.state == "failed"
        ]
        message = (
            f"[taxi_pipeline] Run {context['run_id']} FAILED.\n"
            f"Failed tasks: {failed_tasks}\n"
            f"Logical date: {context['logical_date']}"
        )
        logger.error(message)
        # TODO: replace with email/Slack notification in production
        # e.g. send_slack_alert(message) or send_email(message)

    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=_alert_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # -----------------------------------------------------------------------
    # DAG dependency chain
    # -----------------------------------------------------------------------
    (
        download_data
        >> load_to_snowflake
        >> dbt_run
        >> dbt_test
        >> quality_checks
        >> log_metrics
        >> alert_on_failure
    )
