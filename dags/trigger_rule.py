from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2021, 4, 29),
}
with DAG(
    'trigger_rule',
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
) as dag:
    task_1 = BashOperator(
        task_id='task_1',
        bash_command='exit 1',
    )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='exit 0',
    )

    task_3 = BashOperator(
        task_id='task_3',
        bash_command='exit 0',
        trigger_rule='one_failed',
    )

    [task_1, task_2] >> task_3
