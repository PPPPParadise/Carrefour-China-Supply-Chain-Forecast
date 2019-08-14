import datetime
import warnings

warnings.filterwarnings('ignore')

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
    'start_date': datetime.datetime(2019, 1, 1),
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

forecast_orderflow = DAG('forecast_orderflow',
                         schedule_interval='30 6 * * *',
                         default_args=default_args, catchup=False)


def python_load_data(ds, **kwargs):
    run_date = kwargs['ds_nodash']
    tomorrow_date = kwargs['tomorrow_ds_nodash']

    data_loading_process(run_date, tomorrow_date, project_folder)


def python_dm_order(ds, **kwargs):
    dm_order_process(kwargs['tomorrow_ds_nodash'])


def python_store_order_file(ds, **kwargs):
    date_str = kwargs['tomorrow_ds_nodash']
    store_order_file_process(date_str, project_folder + "/order_files", order_output_folder,
                             store_order_filename.format(date_str))


def python_dc_order_file(ds, **kwargs):
    date_str = kwargs['tomorrow_ds_nodash']
    dc_order_file_process(date_str, project_folder + "/order_files", order_output_folder,
                          dc_order_filename.format(date_str))


def show_dag_args(ds, **kwargs):
    logFile = open(f'{project_folder}/log/forecast_orderflow_run.txt', "a")

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
                           dag=forecast_orderflow)

run_dm_order = PythonOperator(task_id='run_dm_order',
                              python_callable=python_dm_order,
                              provide_context=True,
                              dag=forecast_orderflow)

run_onstock_store_order = BashOperator(
    task_id='run_onstock_store_order',
    bash_command='spark-submit --class "carrefour.forecast.process.OnStockForecastProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_orderflow,
)

run_xdock_order = BashOperator(
    task_id='run_xdock_order',
    bash_command='spark-submit --class "carrefour.forecast.process.XDockingForecastProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_orderflow,
)

run_dc_order = BashOperator(
    task_id='run_dc_order',
    bash_command='spark-submit --class "carrefour.forecast.process.DcForecastProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
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

show_dag_args = PythonOperator(task_id="show_dag_args",
                               python_callable=show_dag_args,
                               provide_context=True,
                               dag=forecast_orderflow)

load_data.set_upstream(show_dag_args)
run_dm_order.set_upstream(load_data)
run_onstock_store_order.set_upstream(run_dm_order)
run_xdock_order.set_upstream(run_dm_order)
run_dc_order.set_upstream(run_onstock_store_order)
generate_store_order_file.set_upstream(run_onstock_store_order)
generate_store_order_file.set_upstream(run_xdock_order)
generate_dc_order_file.set_upstream(run_dc_order)