# Dockerfile
FROM apache/airflow:2.10.3-python3.12 as base

# Install system dependencies
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to airflow user for security
USER airflow
WORKDIR /opt/airflow

# Install Python dependencies
COPY --chown=airflow:root docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

# Development stage for live mounting
FROM base as dev
# Keep this empty for development purposes

# Production stage
FROM base as prod
# Verify directories exist and copy with correct permissions
COPY --chown=airflow:root airflow/dags /opt/airflow/dags/
COPY --chown=airflow:root airflow/plugins /opt/airflow/plugins/
COPY --chown=airflow:root airflow/dbt /opt/airflow/dbt/

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=30s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"

# Set dbt environment variables
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV DBT_PROJECT_DIR=/opt/airflow/dbt

# Verify installation and paths
RUN airflow version \
    && dbt --version \
    && test -d /opt/airflow/dags \
    && test -d /opt/airflow/plugins \
    && test -d /opt/airflow/dbt
