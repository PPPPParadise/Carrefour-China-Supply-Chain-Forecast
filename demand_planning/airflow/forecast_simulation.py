import datetime
import warnings

warnings.filterwarnings('ignore')

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from py_scripts.forecast_data_loading import data_loading_process
from py_scripts.forecast_dm_simulation import dm_order_simulation

project_folder = Variable.get("project_folder").strip()
order_process_jar = Variable.get("order_process_jar").strip()

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 6, 17),
    'end_date': datetime.datetime(2019, 8, 5),
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}


def python_load_data(ds, **kwargs):
    run_date = kwargs['ds_nodash']
    tomorrow_date = kwargs['tomorrow_ds_nodash']

    data_loading_process(run_date, tomorrow_date, project_folder)


def python_dm_simulation(ds, **kwargs):
    dm_order_simulation(kwargs['tomorrow_ds_nodash'])


def python_record_simulation_progress(ds, **kwargs):
    logFile = open(f'/data/jupyter/ws_house/airflow/simulation_run.txt', "a")

    logFile.write(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))
    logFile.write(" ds: ")
    logFile.write(ds)
    logFile.write("Finish \n")

    logFile.close()


forecast_simulation = DAG('forecast_simulation',
                          schedule_interval='30 6 * * *',
                          default_args=default_args, catchup=False)

task_load_data = PythonOperator(task_id='load_data',
                                python_callable=python_load_data,
                                provide_context=True,
                                dag=forecast_simulation)

run_dm_simulation = PythonOperator(task_id='run_dm_simulation',
                                   python_callable=python_dm_simulation,
                                   provide_context=True,
                                   dag=forecast_simulation)

run_onstock_store_simulation = BashOperator(
    task_id='run_onstock_store_simulation',
    bash_command='spark-submit --class "carrefour.forecast.process.OnStockSimulationProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_simulation,
)

run_xdock_simulation = BashOperator(
    task_id='run_xdock_simulation',
    bash_command='spark-submit --class "carrefour.forecast.process.XDockingSimulationProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_simulation,
)

run_dc_simulation = BashOperator(
    task_id='run_dc_simulation',
    bash_command='spark-submit --class "carrefour.forecast.process.DcSimulationProcess" --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_simulation,
)

record_simulation_progress = PythonOperator(task_id="record_simulation_progress",
                                            python_callable=python_record_simulation_progress,
                                            provide_context=True,
                                            dag=forecast_simulation)

run_dm_simulation.set_upstream(task_load_data)
run_onstock_store_simulation.set_upstream(run_dm_simulation)
run_xdock_simulation.set_upstream(run_dm_simulation)
run_dc_simulation.set_upstream(run_onstock_store_simulation)
record_simulation_progress.set_upstream(run_xdock_simulation)
record_simulation_progress.set_upstream(run_dc_simulation)
