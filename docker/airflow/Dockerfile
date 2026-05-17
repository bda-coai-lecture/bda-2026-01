ARG AIRFLOW_IMAGE=apache/airflow:3.2.1-python3.12
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir "astronomer-cosmos>=1.11,<2.0" "dbt-bigquery>=1.9,<2.0" \
    && python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-bigquery>=1.9,<2.0"
