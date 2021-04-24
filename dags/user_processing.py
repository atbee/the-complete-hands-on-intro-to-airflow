import json
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

default_args = {
    'start_date': datetime(2021, 1, 1),
}
with DAG(
    'user_processing',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start')

    creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE users (
                email TEXT NOT NULL PRIMARY KEY,
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL
            );
            ''',
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
    )

    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    end = DummyOperator(task_id='end')

    start >> creating_table >> is_api_available >> extracting_user >> end
