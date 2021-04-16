from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries':3
}


with DAG('dag_sensors',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    task_print_hello_world = BashOperator(  task_id='task_print_hello_world',
                                            bash_command='echo "Hello World!" > /opt/airflow/logs/hello.txt')

    task_remove_hello_world = BashOperator(  task_id='task_remove_hello_world',
                                             bash_command='rm /opt/airflow/logs/hello.txt')


    task_waiting_for_data = FileSensor( task_id='task_waiting_for_data',
                                        fs_conn_id='fs_path_default',
                                        filepath='hello.txt',
                                        poke_interval=15)

    end = DummyOperator(task_id='end')


start << [task_print_hello_world,task_waiting_for_data] << task_remove_hello_world << end