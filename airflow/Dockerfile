FROM apache/airflow:2.10.2-python3.11

# Airflow-compatible packages (no dbt here — protobuf conflict)
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# dbt gets its own isolated venv to avoid the protobuf conflict
USER root
RUN python3.11 -m venv /dbt-venv
COPY requirements-dbt.txt /requirements-dbt.txt
RUN /dbt-venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt
USER airflow
