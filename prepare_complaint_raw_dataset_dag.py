from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from functools import reduce
from textwrap import dedent

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
dag = DAG(dag_id='End_to_End_Data_Ingestion_Pipeline',
          default_args=default_args,
          catchup=False,
          schedule_interval= timedelta(days=1))
pyspark_app_home = Variable.get("PYSPARK_APP_HOME")

def to_application_args(app_args: dict):
    return list(reduce(lambda x, y: x + y, app_args.items()))


# activate_env_operator = BashOperator(task_id='activate_env_task',
#                               bash_command=f'conda activate venv_pyspark',
#                               dag=dag)

build_operator = BashOperator(task_id='prepare_build_task', 
                            bash_command=f'bash {pyspark_app_home}/deploy.sh ', 
                            dag=dag)

wait_operator = BashOperator(task_id='wait_task_for_creating_lineage_1',
                            bash_command=f'sleep 10 ',
                            dag=dag)


def get_spark_operation_definition(org):
    op = SparkSubmitOperator(task_id=f'{org}___prepare_complaint_raw_dataset',
                            conn_id='Spark_Miniconda_Venv_pyspark',
                            py_files=f'{pyspark_app_home}/dist/jobs.zip,{pyspark_app_home}/dist/libs.zip',
                            application=f'{pyspark_app_home}/dist/main.py',
                            total_executor_cores=4,
                            name=f'{org}___prepare_complaint_raw_dataset',
                            application_args=to_application_args({'--job': 'prepare_complaint_raw_dataset'}),
                            dag=dag)
    return op

spark_operations = [get_spark_operation_definition(org) for org in ['Amazon', 'Kenstar', 'Panasonic']]
build_operator >> wait_operator >> spark_operations