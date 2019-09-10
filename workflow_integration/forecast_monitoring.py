import datetime
import warnings
from datetime import timedelta

warnings.filterwarnings('ignore')
from impala.dbapi import connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

project_folder = Variable.get("project_folder").strip()
record_folder = Variable.get("record_folder").strip()
monitoring_output_folder = Variable.get("monitoring_output_folder").strip()

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 8, 19),
    'end_date': datetime.datetime(2029, 8, 5),
    'depends_on_past': True,
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15)
}



forecast_monitoring = DAG('forecast_monitoring',
                          schedule_interval='01 0 * * *',
                          default_args=default_args,
                          max_active_runs=1,
                          catchup=False)

run_monitoring = BashOperator(
    task_id='run_monitoring',
    wait_for_downstream=True,
    bash_command="MONITOR_RUN_DATE='{{ ds_nodash }}' jupyter nbconvert --execute " + project_folder + '/monitor_checks.ipynb --to=html --output=' + record_folder + '/monitoring/report_monitor_{{ ds_nodash }}.html --ExecutePreprocessor.timeout=900 --no-input',
    dag=forecast_monitoring
)

copy_output = BashOperator(
    task_id='copy_output',
    wait_for_downstream=True,
    bash_command='cp -r ' + record_folder + '/monitoring/*_{{ ds_nodash }}.* ' + monitoring_output_folder,
    dag=forecast_monitoring
)

copy_output.set_upstream(run_monitoring)