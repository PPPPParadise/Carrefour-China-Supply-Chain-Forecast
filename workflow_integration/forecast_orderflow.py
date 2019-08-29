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
from py_scripts.forecast_data_loading import calculate_service_level_process
from py_scripts.forecast_dm_order import dm_order_process
from py_scripts.forecast_order_file import store_order_file_process
from py_scripts.forecast_order_file import dc_order_file_process
from py_scripts.forecast_check_missing_orders import store_missing_order_process
from py_scripts.forecast_check_missing_orders import dc_missing_order_process



project_folder = Variable.get("project_folder").strip()
record_folder = Variable.get("record_folder").strip()
log_folder = Variable.get("log_folder").strip()
order_output_folder = Variable.get("order_output_folder").strip()
forecast_output_folder = Variable.get("forecast_output_folder").strip()
store_order_filename = Variable.get("store_order_filename").strip()
store_highvalue_order_filename = Variable.get("store_highvalue_order_filename").strip()
dc_order_filename = Variable.get("dc_order_filename").strip()
store_missing_order_filename = Variable.get("store_missing_order_filename").strip()
dc_missing_order_filename = Variable.get("dc_missing_order_filename").strip()
order_process_jar = Variable.get("order_process_jar").strip()

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 8, 11),
    'end_date': datetime.datetime(2049, 1, 1),
    'depends_on_past': True,
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}


forecast_orderflow = DAG('forecast_orderflow',
          schedule_interval='30 23 * * *',
          max_active_runs = 1,
          default_args=default_args, catchup=False)

def get_order_day(tomorrow_ds):
    true_tomorrow = datetime.datetime.strptime(tomorrow_ds, '%Y%m%d').date() + timedelta(days=1)
    return true_tomorrow.strftime("%Y%m%d")

def python_load_data(ds, **kwargs):
    stock_day = kwargs['tomorrow_ds_nodash']
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    
    data_loading_process(stock_day, order_day, project_folder)
    

def python_calculate_service_level(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    
    calculate_service_level_process(order_day, project_folder)
    
def python_dm_order(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    dm_order_process(order_day)
    
    
def python_store_order_file(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    store_order_file_process(order_day, record_folder + "/order_files", order_output_folder, store_order_filename.format(order_day), store_highvalue_order_filename.format(order_day))
    
    
def python_dc_order_file(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    dc_order_file_process(order_day, record_folder + "/order_files", order_output_folder, dc_order_filename.format(order_day))
    
def python_store_missing_order_file(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    store_missing_order_process(order_day, record_folder + "/order_checks", order_output_folder, store_missing_order_filename.format(order_day))
    
    
def python_dc_missing_order_file(ds, **kwargs):
    order_day = get_order_day(kwargs['tomorrow_ds_nodash'])
    dc_missing_order_process(order_day, record_folder + "/order_checks", order_output_folder, dc_missing_order_filename.format(order_day))


def show_dag_args(ds, **kwargs):
    logFile = open(f'{log_folder}/forecast_orderflow/ds_{ds}/run_parameter.log', "a")
    
    logFile.write(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))
    logFile.write(" ds is ")
    logFile.write(ds)
    logFile.write("\n Other parameters \n")
    logFile.write(str(kwargs))
    logFile.write("\n End \n\n")
    
    logFile.close()
    
load_data = PythonOperator(task_id='load_data',
                             python_callable=python_load_data,
                             provide_context=True,
                             wait_for_downstream=True,
                             dag=forecast_orderflow)

calculate_service_level = PythonOperator(task_id='calculate_service_level',
                             python_callable=python_calculate_service_level,
                             provide_context=True,
                             wait_for_downstream=True,
                             dag=forecast_orderflow)

#run_dm_order = PythonOperator(task_id='run_dm_order',
#                             python_callable=python_dm_order,
#                             provide_context=True,
#                             wait_for_downstream=True,
#                             dag=forecast_orderflow)

run_onstock_store_order = BashOperator(
    task_id='run_onstock_store_order',
    wait_for_downstream=True,
    bash_command='spark-submit --class "carrefour.forecast.process.OnStockForecastProcess" --master yarn --driver-memory 6G --num-executors 14 ' + order_process_jar + ' {{ tomorrow_ds_nodash }} day_shift=1 >> ' + log_folder + '/forecast_orderflow/ds_{{ ds }}/run_onstock_store_order.log',
    dag=forecast_orderflow,
)


run_xdock_order = BashOperator(
    task_id='run_xdock_order',
    wait_for_downstream=True,
    bash_command='spark-submit --class "carrefour.forecast.process.XDockingForecastProcess" --master yarn --driver-memory 6G --num-executors 14 ' + order_process_jar + ' {{ tomorrow_ds_nodash }} day_shift=1 >> ' + log_folder + '/forecast_orderflow/ds_{{ ds }}/run_xdock_order.log',
    dag=forecast_orderflow,
)


run_dc_order = BashOperator(
    task_id='run_dc_order',
    wait_for_downstream=True,
    bash_command='spark-submit --class "carrefour.forecast.process.DcForecastProcess" --master yarn --driver-memory 6G --num-executors 14 ' + order_process_jar + ' {{ tomorrow_ds_nodash }} day_shift=1 >> ' + log_folder + '/forecast_orderflow/ds_{{ ds }}/run_dc_order.log',
    dag=forecast_orderflow,
)


generate_store_order_file = PythonOperator(task_id='generate_store_order_file',
                             python_callable=python_store_order_file,
                             provide_context=True,
                             dag=forecast_orderflow)


generate_dc_order_file = PythonOperator(task_id='generate_dc_order_file',
                             python_callable=python_dc_order_file,
                             provide_context=True,
                             dag=forecast_orderflow)

check_store_order = PythonOperator(task_id='check_store_order',
                             python_callable=python_store_missing_order_file,
                             provide_context=True,
                             dag=forecast_orderflow)


check_dc_order = PythonOperator(task_id='check_dc_order',
                             python_callable=python_dc_missing_order_file,
                             provide_context=True,
                             dag=forecast_orderflow)



show_dag_args = PythonOperator(task_id="show_dag_args",
                             python_callable=show_dag_args,
                             provide_context=True,
                             wait_for_downstream=True,
                             dag=forecast_orderflow)


load_data.set_upstream(show_dag_args)
calculate_service_level.set_upstream(load_data)
#run_dm_order.set_upstream(calculate_service_level)
run_onstock_store_order.set_upstream(calculate_service_level)
run_xdock_order.set_upstream(calculate_service_level)
run_dc_order.set_upstream(run_onstock_store_order)
generate_store_order_file.set_upstream(run_onstock_store_order)
generate_store_order_file.set_upstream(run_xdock_order)
generate_dc_order_file.set_upstream(run_dc_order)
check_store_order.set_upstream(generate_store_order_file)
check_dc_order.set_upstream(generate_dc_order_file)