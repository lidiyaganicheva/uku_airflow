######################
# Imports
######################
import json
import logging
from datetime import datetime

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

######################
# Variables
######################


dag_id = "weather_dag"
start_date = datetime(2025, 5, 6)
schedule = "@daily"

dag_args = {
    'job_name': dag_id,
    'owner': 'airflow',
}

# Add more cities: Kyiv, Kharkiv, Odesa, and Zhmerynka
CITIES = ["Lviv,UA", "Kyiv,UA", "Odesa,UA", "Kharkiv,UA", "Zhmerynka,UA"]


#####################
# Functions
#####################
def _process_weather(ti, city):
    info = ti.xcom_pull(f"weather_pipeline.extract_data_{city}").get("current")
    timestamp = ti.execution_date
    temp = info.get("temp")
    # Add humidity, cloudiness, and wind speed
    humidity = info.get("humidity")
    cloudiness = info.get("clouds")
    wind_speed = info.get("wind_speed")
    return timestamp, temp, humidity, cloudiness, wind_speed


######################
# Operators
######################
with DAG(dag_id=dag_id,
         schedule_interval=schedule,
         start_date=start_date,
         catchup=True,
         default_args=dag_args) as dag:
    b_create = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="postgres_default",
        sql="""
         CREATE TABLE IF NOT EXISTS measures
         (
        timestamp TIMESTAMP,
        city VARCHAR(255),
        temp REAL,
        humidity REAL,
        cloudiness REAL,
        wind_speed REAL
         );"""
    )

    check_geocoding_api = HttpSensor(
        task_id="check_geocoding_api",
        http_conn_id="weather_conn",
        endpoint="geo/1.0/direct",
        request_params={"appid": Variable.get("WEATHER_API_KEY"),
                        "q": "Lviv,UA", "limit": 1}
    )

    with TaskGroup(group_id=f"weather_pipeline") as tg:
        for city in CITIES:
            city_name = city.split(",")[0]

            extract_geocoding_data = HttpOperator(
                task_id=f"extract_geocoding_data_{city_name}",
                http_conn_id="weather_conn",
                endpoint="geo/1.0/direct",
                data={"appid": Variable.get("WEATHER_API_KEY"),
                      "q": city,
                      "limit": 1},
                method="GET",
                response_filter=lambda response: json.loads(response.text) or [{}],
                log_response=True,
                do_xcom_push=True
            )

            extract_data = HttpOperator(
                task_id=f"extract_data_{city_name}",
                http_conn_id="weather_conn",
                # Change API version from 2.5 to 3.0
                endpoint="data/3.0/onecall",
                data={"appid": Variable.get("WEATHER_API_KEY"),
                      "lat": "{{ ti.xcom_pull(task_ids='weather_pipeline.extract_geocoding_data_" + city_name + "')[0]['lat']}}",
                      "lon": "{{ ti.xcom_pull(task_ids='weather_pipeline.extract_geocoding_data_" + city_name + "')[0]['lon']}}",
                      # Add access to historical data
                      "dt": "{{ execution_date.timestamp() | int }}",
                      "exclude": "minutely,hourly,daily,alerts"
                      },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                do_xcom_push=True,
                log_response=True,
            )

            process_data = PythonOperator(
                task_id=f"process_data_{city_name}",
                python_callable=_process_weather,
                op_kwargs={"city": city_name},
            )

            inject_data = SQLExecuteQueryOperator(
                task_id=f"inject_data_{city_name}",
                conn_id="postgres_default",
                sql="""
                INSERT INTO measures 
                (timestamp, city, temp, humidity, cloudiness, wind_speed) VALUES
                ('{{ti.xcom_pull(task_ids='weather_pipeline.process_data_""" + city_name + """')[0]}}',
                '""" + city_name + """',
                {{ti.xcom_pull(task_ids='weather_pipeline.process_data_""" + city_name + """')[1]}},
                {{ti.xcom_pull(task_ids='weather_pipeline.process_data_""" + city_name + """')[2]}},
                {{ti.xcom_pull(task_ids='weather_pipeline.process_data_""" + city_name + """')[3]}},
                {{ti.xcom_pull(task_ids='weather_pipeline.process_data_""" + city_name + """')[4]}}
                );
                """,
            )
            extract_geocoding_data >> extract_data >> process_data >> inject_data

######################
# DAGs flow
######################
b_create >> check_geocoding_api >> tg
