from datetime import datetime
from airflow.utils import dates
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor

DAG_NAME = 'python_dinamic_tasks'
default_args = {
    'owner': 'Emanuel Baquero',
    'start_date': dates.days_ago(1)
}


with DAG(dag_id=DAG_NAME,
         default_args=default_args,
         schedule_interval='@daily') as dag:

    operatos_list = []

    start = DummyOperator(task_id='start')
    operatos_list.append(start)
    for i in ['Argentina','Chile','Brasil','Peru']:
        imprimir = BashOperator(task_id=f'imprimir_{i}',
                                bash_command=f'echo {i}')
        operatos_list.append(imprimir)
    end = DummyOperator(task_id='end', trigger_rule='all_done')
    operatos_list.append(end)



for i in range(len(operatos_list)-1):
    operatos_list[i] >> operatos_list[i+1]


