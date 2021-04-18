from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator  import BashOperator
from datetime import datetime
from airflow.utils import dates
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftOperator
from airflow.operators.redshift_to_s3_operator import RedshiftToS3Operator



default_args = {
	'owner':'Emanuel Baquero',
	'start_date':dates.days_ago(1),
	's3_bucket':'bucket_normal',
	'redshift_conn_id':'redshift_normal',
	'schema':'public',
	'table':'{{ task.task_id }}',
	's3_key':'datos_{{ task.task_id }}_{{ ts_nodash }}'
}


# DAG
with DAG(dag_id='dag_ejercicio',
		 default_args=default_args,
		 schedule_interval = '@daily') as dag:


	#TASK 1
	start = DummyOperator(task_id = 'start')

	#TASK 2
	usuarios_ayer = RedshiftToS3Operator(task_id='usuarios_ayer')

	#TASK 3
	compras_ayer = RedshiftToS3Operator(task_id='compras_ayer')

	#TASK 4
	usuarios_hoy = S3ToRedshiftOperator(task_id='usuarios_hoy')

	#TASK 5
	compras_hoy = S3ToRedshiftOperator(task_id='compras_hoy')

	#TASK 6
	py_procesar_compras = BashOperator(task_id='py_procesar_compras', bash_command='python3 /opt/airflow/dags/{{ task.task_id[3:] }}.py')

	#TASK 7
	end = DummyOperator(task_id = 'end')


usuarios_ayer >> usuarios_hoy
compras_ayer >> compras_hoy
start >> [usuarios_hoy,compras_hoy] >> py_procesar_compras >> end

#start >> py_procesar_compras >> end
#RedshiftToS3Operator(task_id='tu_vieja', schema='public', s3_bucket='bucket_normal',s3_key='datos_{{ task.task_id }}_{{ ts_nodash }}',table='{{ task.task_id }}')