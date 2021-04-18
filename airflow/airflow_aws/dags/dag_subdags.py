from datetime import datetime
from airflow.utils import dates
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor

default_args = {
    'owner': 'Emanuel Baquero',
    'start_date': dates.days_ago(1)
}

PARENT_DAG_NAME = 'dag_de_subdags'
TASK_ID_SUBDAG = 'subdag_en_el_dag'
CHILD_DAG_NAME = PARENT_DAG_NAME+'.'+TASK_ID_SUBDAG




def load_subdag(param_PARENT_DAG_NAME, param_CHILD_DAG_NAME, param_default_args):
  with DAG( param_CHILD_DAG_NAME,
            default_args=param_default_args,
            schedule_interval='@daily') as subdag:

    start_subdag = BashOperator(task_id='sub_start', bash_command='echo sub_start')

    end_subdag = BashOperator(task_id='sub_end', bash_command='echo sub_end')

  [start_subdag,end_subdag]

  return subdag




with DAG(PARENT_DAG_NAME,
         default_args=default_args,
         schedule_interval='@daily') as dag:

    start = BashOperator(task_id='super_start', bash_command='echo super_start')

    subdag = SubDagOperator(  task_id=TASK_ID_SUBDAG,
                              subdag=load_subdag(PARENT_DAG_NAME,CHILD_DAG_NAME,dag.default_args)
                            )

    end = BashOperator(task_id='super_end', bash_command='echo super_end')

start >> subdag >> end
