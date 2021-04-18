from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.models import DAG

default_args = {
    'owner': 'Emanuel Baquero',
    'start_date': dates.days_ago(1)
}
def mostrar_parametros(**kwargs):
    print(kwargs['dag_run'].conf)


## TASKS
with DAG(dag_id='dag_pasando_parametros',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    start = DummyOperator(task_id='start')


    bash_task = BashOperator(
            task_id='prueba_bash_con_parametros',
            bash_command="echo {{ dag_run.conf['var_prueba'] or 'chau mundo' }}"
    )

    py_task = PythonOperator(
            task_id='prueba_py_con_parametros',
            python_callable=mostrar_parametros,
            provide_context=True
    )

    end = DummyOperator(task_id='end')



## SCHEDULER
start >> bash_task >> py_task >> end


