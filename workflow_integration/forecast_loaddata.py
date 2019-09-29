import datetime
import warnings
from datetime import timedelta

warnings.filterwarnings('ignore')
from impala.dbapi import connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from py_scripts.forecast_data_loading import data_loading_process
from py_scripts.forecast_dm_order import dm_order_process
from py_scripts.forecast_order_file import store_order_file_process
from py_scripts.forecast_order_file import dc_order_file_process


project_folder = Variable.get("project_folder").strip()
order_output_folder = Variable.get("order_output_folder").strip()
store_order_filename = Variable.get("store_order_filename").strip()
dc_order_filename = Variable.get("dc_order_filename").strip()
order_process_jar = Variable.get("order_process_jar").strip()

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 1, 8),
    'end_date': datetime.datetime(2049, 1, 1),
    'depends_on_past': True,
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15)
}


forecast_loaddata = DAG('forecast_loaddata',
          schedule_interval='10 0 * * *',
          max_active_runs = 1,
          default_args=default_args, catchup=False)


def python_load_data(ds, **kwargs):
    run_date = kwargs['ds_nodash']
    tomorrow_date = kwargs['tomorrow_ds_nodash']
    
    data_loading_process(run_date, tomorrow_date, project_folder)


load_data = PythonOperator(task_id='load_data',
                             python_callable=python_load_data,
                             provide_context=True,
                             dag=forecast_loaddata)

