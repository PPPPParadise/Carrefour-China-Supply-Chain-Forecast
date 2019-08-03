import datetime
import warnings
from datetime import timedelta

warnings.filterwarnings('ignore')
from impala.dbapi import connect

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

project_folder = Variable.get("project_folder")
order_output_folder = Variable.get("order_output_folder")
store_order_file = Variable.get("store_order_file_name")

default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 4, 1),
    'email': ['house.gong@artefact.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'end_date': datetime.datetime(2049, 1, 1),
}

def read_sql(sql_file_path):
    with open(project_folder + "/sql/" + sql_file_path) as f:
        sql = f.read()
    return sql

def run_sql_with_impala(sql, run_date):
    sql = sql.format(run_date)
    print(sql)

    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database='vartefact') as conn:
        curr = conn.cursor()
        curr.execute(sql)
            
forecast_orderflow = DAG('forecast_orderflow',
          schedule_interval='30 0 * * *',
          default_args=default_args, catchup=False)

def python_load_data(ds, **kwargs):
    
    job_date = datetime.datetime.strptime(ds, '%Y-%m-%d').date()
    run_date = job_date 
    #+ timedelta(days=-1)

    print('Load data for date', run_date.strftime("%Y%m%d"))

    print("Load forecast_item_code_id_stock")
    sql = read_sql("forecast_item_code_id_stock.sql")
    run_sql_with_impala(sql, run_date.strftime("%Y%m%d"))
    
    print("Load forecast_dc_latest_sales")
    sql = read_sql("forecast_dc_latest_sales.sql")
    run_sql_with_impala(sql, run_date.strftime("%Y%m%d"))
    
    print("Load forecast_p4cm_store_item")
    sql = read_sql("forecast_p4cm_store_item.sql")
    run_sql_with_impala(sql, run_date.strftime("%Y%m%d"))
    
    print("Load forecast_lfms_daily_dcstock")
    sql = read_sql("forecast_lfms_daily_dcstock.sql")
    run_sql_with_impala(sql, run_date.strftime("%Y%m%d"))
    
    print("All Loaded")
    

load_data = PythonOperator(task_id="load_data",
                             python_callable=python_load_data,
                             provide_context=True,
                             dag=forecast_orderflow)


