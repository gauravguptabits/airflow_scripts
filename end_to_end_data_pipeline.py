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


def to_application_args(app_args: dict):
    return list(reduce(lambda x, y: x + y, app_args.items()))


def _return_branch():
    task_list = [
        'panasonic_text_extraction_task',
        'sony_text_extraction_task',
        'hitachi_text_extraction_task'
    ]
    return random.choices(task_list)

with DAG(dag_id='End_To_End_DataSnapshot_Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval= timedelta(days=1)) as dag:

    tenant_selector_task = BranchPythonOperator(task_id='tenant_selector', python_callable=_return_branch)

    panasonic_text_extraction_task = SparkSubmitOperator(task_id=f'panasonic_text_extraction_task',
                                                          conn_id='Spark_Miniconda_Venv_pyspark',
                                                          py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                          application=f'{pyspark_app_home}/dist/main.py',
                                                          total_executor_cores=4,
                                                          name=f'panasonic_text_extraction_task',
                                                          application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                          dag=dag)

    sony_text_extraction_task = SparkSubmitOperator(task_id=f'sony_text_extraction_task',
                                                         conn_id='Spark_Miniconda_Venv_pyspark',
                                                         py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                         application=f'{pyspark_app_home}/dist/main.py',
                                                         total_executor_cores=4,
                                                         name=f'sony_text_extraction_task',
                                                         application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                         dag=dag)

    hitachi_text_extraction_task = SparkSubmitOperator(task_id=f'hitachi_text_extraction_task',
                                                         conn_id='Spark_Miniconda_Venv_pyspark',
                                                         py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                         application=f'{pyspark_app_home}/dist/main.py',
                                                         total_executor_cores=4,
                                                         name=f'hitachi_text_extraction_task',
                                                         application_args=to_application_args(
                                                              {'--job': 'prepare_complaint_raw_dataset'}),
                                                         dag=dag)

    notify_task = BashOperator(task_id='notification_task', bash_command=f'sleep 2 ', dag=dag)
    tenant_selector_task >> [panasonic_text_extraction_task, sony_text_extraction_task, hitachi_text_extraction_task]
    [   panasonic_text_extraction_task, 
        sony_text_extraction_task,
        hitachi_text_extraction_task
    ] >> notify_task
    # end of script.