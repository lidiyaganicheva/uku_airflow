#!/bin/bash

echo "Initializing Airflow connections and variables..."

# 1. GOOGLE CLOUD CONNECTION
airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra '{
        "extra__google_cloud_platform__project": "crucial-subset-437000-g0",
        "extra__google_cloud_platform__key_path": "/opt/airflow/dags/crucial-subset-437000-g0-fab5b1f77634.json"
    }'

# 2. OPENAI CONNECTION
airflow connections add 'openai_default' \
    --conn-type 'openai' \
    --conn-password "$CHATGPT_KEY" \


# 3. POSTGRES CONNECTION
airflow connections add 'postgres_storage' \
    --conn-type 'postgres' \
    --conn-login 'admin' \
    --conn-password 'admin' \
    --conn-host 'postgres_storage' \
    --conn-port '5432' \
    --conn-schema 'default'

# 4. VARIABLE: filtered_files
airflow variables set 'filtered_files' '[]'
