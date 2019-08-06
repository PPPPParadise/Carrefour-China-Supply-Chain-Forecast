import datetime
import warnings
from datetime import timedelta

warnings.filterwarnings('ignore')
from impala.dbapi import connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from forecast_dm_order import run_dm_order

project_folder = Variable.get("project_folder")
order_output_folder = Variable.get("order_output_folder")
store_order_file = Variable.get("store_order_file_name")
order_process_jar = Variable.get("order_process_jar")

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 8, 1),
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

def read_sql(sql_file_path):
    with open(project_folder + "/sql/" + sql_file_path) as f:
        sql = f.read()
    return sql

def run_sql_with_impala(sql):
    print(sql)

    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database='vartefact') as conn:
        curr = conn.cursor()
        curr.execute(sql)
            
forecast_orderflow = DAG('forecast_orderflow',
          schedule_interval='30 6 * * *',
          default_args=default_args, catchup=False)

def record_load_result(runDateStr, table_name):
    sql = \
    """
    insert into vartefact.forecast_script_runs
    select now(), '{0}', 'Success', 'Load table {1}', 'Data loading', 
    'Run date:{0}', cast(count(*) as STRING), '', '' as cnt
    from {1} where date_key='{0}'
    """.replace("\n", " ")
    
    sql = sql.format(runDateStr, table_name)
    run_sql_with_impala(sql)
    

def python_load_data(ds, **kwargs):

    run_date = kwargs['ds_nodash']
    
    print('Load data for date', run_date)
    
    print('Load forecast_item_code_id_stock')
    sql = read_sql('forecast_item_code_id_stock.sql')
    run_sql_with_impala(sql.format(run_date))
    record_load_result(run_date, 'vartefact.forecast_item_code_id_stock')
    
    print('Load forecast_dc_latest_sales')
    sql = read_sql('forecast_dc_latest_sales.sql')
    run_sql_with_impala(sql.format(run_date))
    record_load_result(run_date, 'vartefact.forecast_dc_latest_sales')
    
    print('Load forecast_p4cm_store_item')
    sql = read_sql('forecast_p4cm_store_item.sql')
    run_sql_with_impala(sql.format(run_date))
    record_load_result(run_date, 'vartefact.forecast_p4cm_store_item')
    
    print('Load forecast_lfms_daily_dcstock')
    sql = read_sql('forecast_lfms_daily_dcstock.sql')
    run_sql_with_impala(sql.format(run_date))
    record_load_result(run_date, 'vartefact.forecast_lfms_daily_dcstock')
    
    print('All Loaded')
    

load_data = PythonOperator(task_id='load_data',
                             python_callable=python_load_data,
                             provide_context=True,
                             dag=forecast_orderflow)

run_xdock_order = BashOperator(
                        task_id='run_xdock_order',
                        bash_command='spark-submit --class ''carrefour.forecast.process.XDockingForecastProcess'' --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ ds_nodash }}',
                        dag=forecast_orderflow
                    )

run_onstock_store_order = BashOperator(
                        task_id='run_onstock_store_order',
                        bash_command='spark-submit --class ''carrefour.forecast.process.OnStockForecastProcess'' --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ ds_nodash }}',
                        dag=forecast_orderflow
                    )

run_onstock_dc_order = BashOperator(
                        task_id='run_onstock_dc_order',
                        bash_command='spark-submit --class ''carrefour.forecast.process.DcForecastProcess'' --master yarn --driver-memory 8G --num-executors 8 ' + order_process_jar + ' {{ ds_nodash }}',
                        dag=forecast_orderflow
                    )

run_dm_order = PythonOperator(task_id='run_dm_order',
                             python_callable=run_dm_order,
                             provide_context=True,
                             dag=forecast_orderflow)

run_onstock_store_order.set_upstream(load_data)
run_xdock_order.set_upstream(load_data)
run_onstock_dc_order.set_upstream(run_onstock_store_order)
run_dm_order.set_upstream(run_onstock_store_order)
run_dm_order.set_upstream(run_xdock_order)