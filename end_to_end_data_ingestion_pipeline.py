from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from functools import reduce
from textwrap import dedent
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from _checkpoint_task import (  data_ingestion_failure_ckpt_hook, 
                                data_ingestion_read_ckpt_hook,
                                data_ingestion_success_ckpt_hook)


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


def _read_ingestion_checkpoint(ti):
    # TODO: Implement a postgres hook here.
    # ti.xcom_push(key='some_key', value='some_value')
    Variable.set('some_key', 'some_value')
    ti.xcom_push(key='some_x_com_key', value='Some_Xcom_Value')
    return

def _check_task_status():
    return 'ckpt_success_update'

def _update_failure_ckpt():
    # TODO: Implement a postgres hook here.
    return

def _update_success_ckpt():
    # TODO: Implement a postgres hook here.
    return

with DAG(dag_id='End_to_End_Data_Ingestion_Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval= timedelta(minutes=30)) as dag:

    # build_operator = BashOperator(task_id='prepare_build_task', 
    #                             bash_command=f'bash {pyspark_app_home}/deploy.sh ', 
    #                             dag=dag)

    ckpt_reader_task = PythonOperator(task_id='ckpt_reader_1', 
                                      python_callable=data_ingestion_read_ckpt_hook)

    op = SparkSubmitOperator(task_id=f'copy_data_task',
                            conn_id='Spark_Miniconda_Venv_pyspark',
                            py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                            application=f'{pyspark_app_home}/dist/main.py',
                            total_executor_cores=4,
                            name=f'prepare_complaint_raw_dataset',
                            application_args=to_application_args({'--job': 'prepare_complaint_raw_dataset'}),
                            dag=dag)

    spark_task_status = BranchPythonOperator(task_id='check_task_status', 
                                            python_callable=_check_task_status,
                                            dag=dag)

    ckpt_success_task = PythonOperator(task_id='ckpt_success_update',
                                       python_callable=data_ingestion_success_ckpt_hook)

    ckpt_failure_task = PythonOperator(task_id='ckpt_failure_update',
                                      python_callable=data_ingestion_failure_ckpt_hook)

    notification_task = BashOperator(task_id='notify_task',
                                  bash_command=f'echo NOTIFICATION_TASK',
                                  dag=dag)

    ckpt_reader_task >> op >> spark_task_status >> [ckpt_success_task, ckpt_failure_task]
    [ckpt_failure_task, ckpt_success_task] >> notification_task
