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
from airflow.operators.dummy_operator import DummyOperator
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
    return random.choices(['deploy_model_task', 'notify_task'])

with DAG(dag_id='End_to_End_ML_Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval= timedelta(days=1)) as dag:

    with TaskGroup(group_id='Denoising_task', dag=dag) as den_task:
        compile_text_denoising_task = BashOperator(task_id='compile_text_denoising_task',
                                                   bash_command=f'bash {pyspark_app_home}/deploy.sh ',
                                                   dag=dag)

        execute_text_denoising_task = SparkSubmitOperator(task_id=f'denoise_complaint_dataset',
                             conn_id='Spark_Miniconda_Venv_pyspark',
                             py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                             application=f'{pyspark_app_home}/dist/main.py',
                             total_executor_cores=4,
                             name=f'denoise_complaint_dataset',
                             application_args=to_application_args(
                                 {'--job': 'denoise_complaint_dataset'}),
                             dag=dag)

        # execute_text_denoising_task = BashOperator(task_id='execute_text_denoising_task',
        #                                                 bash_command=f'sleep 2 ',
        #                                                 dag=dag)
        compile_text_denoising_task >> execute_text_denoising_task

    with TaskGroup(group_id='Vectorization_task', dag=dag) as vec_task:
        compile_text_vectorization_task = BashOperator(task_id='compile_text_vectorization_task',
                                                       bash_command=f'bash {pyspark_app_home}/deploy.sh ',
                                                       dag=dag)

        execute_text_vectorization_task = SparkSubmitOperator(task_id=f'vectorize_complaint_dataset',
                                                          conn_id='Spark_Miniconda_Venv_pyspark',
                                                          py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                                                          application=f'{pyspark_app_home}/dist/main.py',
                                                          total_executor_cores=4,
                                                          name=f'vectorize_complaint_dataset',
                                                          application_args=to_application_args(
                                                              {'--job': 'vectorize_complaint_dataset'}),
                                                          dag=dag)

        compile_text_vectorization_task >> execute_text_vectorization_task

    with TaskGroup(group_id='training_task', dag=dag) as train_task:
        train_model_task = BashOperator(task_id='train_model_task',
                                                       bash_command=f'sleep 2 ',
                                                       dag=dag)

        validate_model_task = BashOperator(task_id='validate_model_task',
                                                       bash_command=f'sleep 2 ',
                                                       dag=dag)
        
        train_model_task >> validate_model_task

    test_model_task = BashOperator(task_id='test_model_task',
                                        bash_command=f'sleep 2 ',
                                        dag=dag)

    deploy_model_task = BashOperator(task_id='deploy_model_task',
                              bash_command=f'sleep 2 ',
                              dag=dag)

    notify_task = BashOperator(task_id='notify_task',
                              bash_command=f'sleep 2 ',
                              dag=dag)
    
    update_endpoint_task = BashOperator(task_id='update_endpoint_task',
                               bash_command=f'sleep 2 ',
                               dag=dag)

    model_score_check_task = BranchPythonOperator(
        task_id='model_score_checker', python_callable=_return_branch)

    den_task >> vec_task >> train_task >> test_model_task >> model_score_check_task >> [deploy_model_task, notify_task]
    [deploy_model_task] >> update_endpoint_task

