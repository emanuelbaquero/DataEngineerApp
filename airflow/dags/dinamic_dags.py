from airflow.utils import dates
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator



DAG_NAME = 'python_dinamic_dags'

def create_dag(dag_id,schedule,default_args,pais):

    dag = DAG(dag_id=dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        start = DummyOperator(task_id='start')
        imprimir_pais = BashOperator(task_id='imprimir',bash_command=f'echo {pais}')
        end = DummyOperator(task_id='end')

    start >> imprimir_pais >> end

    return dag

default_args = {
    'owner': 'Emanuel Baquero',
    'start_date': dates.days_ago(1)
}

paises = ['es','it','fr']

for pais in paises:

    dag_id = f'PAIS_{pais}'

    default_args = {
        'owner': f'airflow - {pais}',
        'start_date': dates.days_ago(1)
    }

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   schedule='@daily',
                                   default_args=default_args,
                                   pais=pais)


