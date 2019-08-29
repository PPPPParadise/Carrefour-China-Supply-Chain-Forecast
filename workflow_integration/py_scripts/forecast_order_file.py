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
from py_scripts.forecast_dm_simulation import dm_order_simulation

project_folder = Variable.get("project_folder").strip()
order_simulation_jar = Variable.get("order_simulation_jar").strip()


default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 6, 17),
    'end_date': datetime.datetime(2019, 8, 5),
    'depends_on_past': True,
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15)
}
    
def python_dm_simulation(ds, **kwargs):
    dm_order_simulation(kwargs['tomorrow_ds_nodash'])
    
def python_end_simulation_progress(ds, **kwargs):
    logFile = open(f'/data/jupyter/ws_house/airflow/simulation_run.txt', "a")
    
    logFile.write(datetime.datetime.now().strftime("%Y%m%d %H:%M:%S"))
    logFile.write(" ds: ")
    logFile.write(ds)
    logFile.write("Finish \n")
    
    logFile.close()
    
    
forecast_simulation = DAG('forecast_simulation',
          schedule_interval='30 6 * * *',
          default_args=default_args, 
                          max_active_runs = 1,
                          catchup=False)


run_dm_simulation = PythonOperator(task_id='run_dm_simulation',
                             python_callable=python_dm_simulation,
                             provide_context=True,
                             dag=forecast_simulation,
                                  wait_for_downstream=True)

run_simulation = BashOperator(
    task_id='run_simulation',
    wait_for_downstream=True,
    bash_command='spark-submit --class "carrefour.forecast.process.SimulationProcess" --master yarn --driver-memory 6G --num-executors 14 ' + order_simulation_jar + ' {{ tomorrow_ds_nodash }}',
    dag=forecast_simulation
)

end_simulation_progress = PythonOperator(task_id="end_simulation_progress",
                             python_callable=python_end_simulation_progress,
                                         wait_for_downstream=True,
                             provide_context=True,
                             dag=forecast_simulation)


run_simulation.set_upstream(run_dm_simulation)
end_simulation_progress.set_upstream(run_simulation)
