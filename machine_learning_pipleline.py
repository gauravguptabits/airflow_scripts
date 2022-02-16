from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from functools import reduce
from airflow.operators.python import BranchPythonOperator
import random
from airflow.operators.dummy import DummyOperator

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'Impressico',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 15, tzinfo=local_tz),
    'email': ['gaurav.gupta@impressico.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")
lib_home = Variable.get("lib_home")

def to_application_args(app_args: dict):
    return list(reduce(lambda x, y: x + y, app_args.items()))


with DAG(dag_id='End_To_End_Machine_learning_Pipeline',
          default_args=default_args,
          catchup=False,
          #schedule_interval= timedelta(days=1)) as dag:
          schedule_interval= '*/20 * * * *') as dag:  
    start = DummyOperator(task_id="start") 
    #initial_task = BashOperator(task_id='initial_task', bash_command=f'sleep 2 ', dag=dag)  
    machine_learning_pipeline_task = SparkSubmitOperator(task_id=f'machine_learning_pipeline_task',
                                                          conn_id='Spark_Miniconda_Venv_pyspark',
                                                          py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                          jars = f'{lib_home}/postgresql-42.24.2.jar',
                                                          packages = 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1', 
                                                          application=f'{pyspark_app_home}/dist/main.py',
                                                          total_executor_cores=20,                                                          
                                                          application_args=to_application_args(
                                                              {'--job': 'complaint_inference'}),
                                                          dag=dag)

   
    notify_task = BashOperator(task_id='notification_task', bash_command=f'sleep 2 ', dag=dag)
    start >> machine_learning_pipeline_task >> notify_task
    # end of script.
