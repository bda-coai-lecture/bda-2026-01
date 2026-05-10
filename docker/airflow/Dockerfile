ARG AIRFLOW_IMAGE=apache/airflow:3.2.1-python3.12
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/airflow/.local/bin:${PATH}"
