from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator  import BashOperator
from datetime import datetime


default_args = {
	'owner':'Emanuel Baquero',
	'start_date':dates.days_ago(1)
}


def hello_word(**context):
	for palabra in ['hello','world']:
		print(palabra)

	task_instance=context['task_instance']
	task_instance.xcom_push(key='clave_prueba',value='valor_prueba')
	return 'Retorno un xCom'



def prueba_pull(**context):


	ti=context['task_instance']
	valor_pull = ti.xcom_pull(task_ids='prueba_python')
	print(valor_pull)

	return valor_pull


# DAG
with DAG(dag_id='dag_de_prueba_ema',
		 default_args=default_args,
		 schedule_interval = '@once') as dag:


	#TASK 1
	start = DummyOperator(task_id = 'start')

	#TASK 2
	prueba_python = PythonOperator(	task_id='prueba_python',
									python_callable=hello_word,
									do_xcom_push=True,
									provide_context=True)
	#TASK 3
	prueba_pull = PythonOperator(	task_id='prueba_pull',
									python_callable=prueba_pull,
									do_xcom_push=True,
									provide_context=True)
	#TASK 4
	prueba_bash = BashOperator(	task_id='prueba_bash',
								bash_command='python3 /opt/airflow/dags/programa.py')
	#TASK 4
	prueba_jinja = BashOperator(	task_id='prueba_jinja',
									bash_command='echo {{ var.value.email }}')

	#TASK 5
	end = DummyOperator(task_id = 'end')


start >> [prueba_python,prueba_pull, prueba_bash, prueba_jinja] >> end

