from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from functools import reduce
from textwrap import dedent
from airflow.utils.task_group import TaskGroup
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


def remove_task_from_list(res,task_list):   
    for element in res[1:]:   
        if element in task_list:
            task_list.remove(element)
    return task_list

def _return_branch():
    task_list = [
        'panasonic_text_extraction_task',
        'sony_text_extraction_task',
        'lg_text_extraction_task'
    ]
    executed_tasks_str = Variable.get("executed_tasks")
    
    executed_tasks_list = executed_tasks_str.split(",")
   
    if len(executed_tasks_list[1:]) == len(task_list):
    	Variable.set("executed_tasks",'')
    	executed_tasks_str = ""
    	task = random.choices(task_list)
    else:
        new_task_list = remove_task_from_list(executed_tasks_list,task_list)
        task = random.choices(new_task_list)

    executed_tasks_str = executed_tasks_str + "," + "".join(task)
    Variable.set("executed_tasks",executed_tasks_str )
    return task

with DAG(dag_id='End_To_End_DataSnapshot_Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval= timedelta(days=1)) as dag:
          #schedule_interval= '*/1 * * * *') as dag:
    initial_task = BashOperator(task_id='initial_task', bash_command=f'sleep 2 ', dag=dag)  
    start = DummyOperator(task_id="start") 

    tenant_selector_task = BranchPythonOperator(task_id='tenant_selector', python_callable=_return_branch)

    panasonic_text_extraction_task = SparkSubmitOperator(task_id=f'panasonic_text_extraction_task',
                                                          conn_id='Spark_Miniconda_Venv_pyspark',
                                                          py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                          jars = f'{lib_home}/postgresql-42.2.24.jar',
                                                          application=f'{pyspark_app_home}/dist/main.py',
                                                          total_executor_cores=20,
                                                          name=f'panasonic_text_extraction_task',
                                                          application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                          dag=dag)

    sony_text_extraction_task = SparkSubmitOperator(task_id=f'sony_text_extraction_task',
                                                         conn_id='Spark_Miniconda_Venv_pyspark',
                                                         py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                         jars = f'{lib_home}/postgresql-42.2.24.jar',
                                                         application=f'{pyspark_app_home}/dist/main.py',
                                                         total_executor_cores=20,
                                                         name=f'sony_text_extraction_task',
                                                         application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                         dag=dag)

    lg_text_extraction_task = SparkSubmitOperator(task_id=f'lg_text_extraction_task',
                                                         conn_id='Spark_Miniconda_Venv_pyspark',
                                                         py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                         jars = f'{lib_home}/postgresql-42.2.24.jar',
                                                         application=f'{pyspark_app_home}/dist/main.py',
                                                         total_executor_cores=20,
                                                         name=f'lg_text_extraction_task',
                                                         application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                         dag=dag)

    notify_task = BashOperator(task_id='notification_task', bash_command=f'sleep 2 ', dag=dag)
    start >> initial_task >> tenant_selector_task >> [panasonic_text_extraction_task, sony_text_extraction_task, lg_text_extraction_task]
    [   panasonic_text_extraction_task, 
        sony_text_extraction_task,
        lg_text_extraction_task
    ] >> notify_task
    # end of script.
