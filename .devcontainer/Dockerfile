FROM apache/airflow:3.0.3-python3.10

USER root

# Install any extra dev tools you want (vim, git, etc.)
RUN apt-get update && apt-get install -y \
    vim less git curl

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Set working dir to airflow dags folder for convenience
WORKDIR /opt/airflow/dags