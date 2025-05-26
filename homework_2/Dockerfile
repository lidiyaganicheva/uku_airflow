FROM apache/airflow:2.10.5

USER airflow
RUN pip install --no-cache-dir \
    google-cloud-storage \
    google-cloud-vision \
    openai \
    psycopg2-binary \
    apache-airflow[google] \
    apache-airflow-providers-openai

USER root
