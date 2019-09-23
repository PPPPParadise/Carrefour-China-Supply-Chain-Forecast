import datetime
import warnings
import os
from datetime import timedelta
from os.path import abspath

warnings.filterwarnings('ignore')
from impala.dbapi import connect
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

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

def timed_pull(query):
    foo = spark.sql(query).toPandas()
    return foo

def read_query(sql_path, create_table=False, kudu_replace=None, **query_params):
    with open(sql_path, 'r') as f:
        query = f.read()
    if kudu_replace is not None:
        for k, v in kudu_replace.items():
            query = query.replace(k, v)
    if not create_table:
        ## remove lines with `table`
        q0 = query
        query = '\n'.join([line for line in q0.split('\n')
                           if ('drop table' not in line.lower())
                           and ('create table' not in line.lower())])
    query = query.format(**query_params)
    return query

def python_check_dc_order(ds, **kwargs):
    run_date = datetime.datetime.strptime(ds, '%Y-%m-%d').date()
    
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    spark = SparkSession.builder \
        .appName("Generate order forecast file") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '8g') \
        .config("spark.executor.memory", '6g') \
        .config("spark.num.executors", '14') \
        .config("hive.exec.compress.output", 'false') \
        .config("spark.sql.crossJoin.enabled", 'true') \
        .config("spark.sql.autoBroadcastJoinThreshold", '-1') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext

    sqlc = SQLContext(sc)
    
    tfmt = '%Y%m%d'
    DATE_END = run_date.strftime(tfmt)
    DATE_START = (run_date - datetime.timedelta(days=1)).strftime(tfmt)
    CHK_DATE = ds
    
    print(f"Check Artefact DC order and real DC order for order days between {DATE_START} and {DATE_END}")
    
    kudu_tables = ['lfms.daily_dctrxn', 'lfms.daily_dcstock', 'lfms.ord']

    for tbl in kudu_tables:
        spark.read.format('org.apache.kudu.spark.kudu') \
        .option('kudu.master', "dtla1apps11:7051,dtla1apps12:7051,dtla1apps13:7051") \
        .option('kudu.table', f'impala::{tbl}') \
        .load() \
        .registerTempTable('{}'.format(tbl.replace('.', '_')))
        
    dc_order_sql = read_query(f'{project_folder}/sql/monitor_DC_order_changes.sql',
                            database_name='vartefact', date_start=DATE_START, date_end=DATE_END,
                            kudu_replace={'lfms.ord': 'lfms_ord'})
    
    dc_order = sqlc.sql(dc_order_sql).toPandas()

    dc_order_chk_day = dc_order[(dc_order.date_key.astype(str) == CHK_DATE)]
    dc_order_changes = dc_order_chk_day[dc_order_chk_day.order_diff != 0].astype(str)

    dc_order_changes.to_excel(f'{record_folder}/monitoring/dc_order_changes_{DATE_END}.xlsx', index=False)
    
    dc_diff_insert = dc_order_changes[['dept_code', 'item_code', 'sub_code', 'qty_per_box', 'holding_code',
                                    'risk_item_unilever', 'rotation', 'supplier_code','warehouse', 
                                    'delivery_date', 'item_subcode_name_local', 'purchase_quantity','main_barcode', 
                                    'service_level', 'order_qty_model', 'basic_order_qty','n_actual_orders',
                                    'order_diff']]

    if dc_diff_insert.shape[0] > 0 :

        sqlc.createDataFrame(dc_diff_insert).createOrReplaceTempView("dc_daily_order_diff")

        dc_orders_diff_sql = """
            INSERT OVERWRITE TABLE vartefact.forecast_monitor_dc_order_diff
            PARTITION (date_key)
            SELECT 
                dept_code ,
                item_code ,
                sub_code ,
                qty_per_box ,
                holding_code ,
                risk_item_unilever ,
                rotation ,
                supplier_code ,
                warehouse ,
                delivery_date ,
                item_subcode_name_local ,
                purchase_quantity ,
                main_barcode ,
                service_level ,
                order_qty_model ,
                basic_order_qty ,
                n_actual_orders ,
                order_diff ,
                {0} as date_key
            FROM dc_daily_order_diff 
        """.replace("\n", " ").format(DATE_END)

        sqlc.sql(dc_orders_diff_sql)

    sc.stop()
    
    
def python_check_store_order(ds, **kwargs):
    run_date = datetime.datetime.strptime(ds, '%Y-%m-%d').date()
    
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    spark = SparkSession.builder \
        .appName("Generate order forecast file") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '8g') \
        .config("spark.executor.memory", '6g') \
        .config("spark.num.executors", '14') \
        .config("hive.exec.compress.output", 'false') \
        .config("spark.sql.crossJoin.enabled", 'true') \
        .config("spark.sql.autoBroadcastJoinThreshold", '-1') \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext

    sqlc = SQLContext(sc)
    
    tfmt = '%Y%m%d'
    DATE_END = run_date.strftime(tfmt)
    DATE_START = (run_date - datetime.timedelta(days=1)).strftime(tfmt)
    CHK_DATE = ds
    
    print(f"Check Artefact store order and real store order for order days between {DATE_START} and {DATE_END}")
    
    store_order_sql = read_query(f'{project_folder}/sql/monitor_store_order_changes.sql', 
                                   database_name='vartefact', date_start=DATE_START, date_end=DATE_END)
    
    store_order = sqlc.sql(store_order_sql).toPandas()

    store_order['order_diff'] =  store_order.order_qty_actual - store_order.order_qty_model  
    store_order_chk_day = store_order[store_order.date_key.astype(str) == DATE_END]
    store_order_changes = store_order_chk_day[store_order_chk_day.order_diff != 0].astype(str)
    store_order_changes.to_excel(f'{record_folder}/monitoring/store_order_changes_{DATE_END}.xlsx', index=False)
    
    store_diff_insert = store_order_changes[['store_code', 'dept_code', 'item_code', 'sub_code',
       'cn_name', 'rotation', 'supplier_code', 'holding_code', 'risk_item_unilever',
       'order_qty_model', 'order_qty_actual', 'order_diff']]
    
    if store_diff_insert.shape[0] > 0 :
        
        sqlc.createDataFrame(store_diff_insert).createOrReplaceTempView("store_daily_order_diff")

        store_orders_diff_sql = """
            INSERT OVERWRITE TABLE vartefact.forecast_monitor_store_order_diff
            PARTITION (date_key)
            SELECT 
                store_code ,
                dept_code ,
                item_code ,
                sub_code ,
                cn_name ,
                rotation ,
                supplier_code ,
                holding_code ,
                risk_item_unilever ,
                order_qty_model ,
                order_qty_actual ,
                order_diff ,
                {0} as date_key
            FROM store_daily_order_diff
        """.replace("\n", " ").format(DATE_END)

        sqlc.sql(store_orders_diff_sql)

    sc.stop()


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

run_consistency = BashOperator(
    task_id='run_consistency',
    wait_for_downstream=True,
    bash_command="MONITOR_RUN_DATE='{{ ds_nodash }}' jupyter nbconvert --execute " + project_folder + '/consistency_check.ipynb --to=html --output=' + record_folder + '/monitoring/report_monitor_{{ ds_nodash }}.html --ExecutePreprocessor.timeout=1800 --no-input',
    dag=forecast_monitoring
)

check_dc_order = PythonOperator(task_id='check_dc_order',
                                   python_callable=python_check_dc_order,
                                   provide_context=True,
                                   dag=forecast_monitoring)

check_store_order = PythonOperator(task_id='check_store_order',
                                   python_callable=python_check_store_order,
                                   provide_context=True,
                                   dag=forecast_monitoring)


copy_output = BashOperator(
    task_id='copy_output',
    wait_for_downstream=True,
    bash_command='cp -r ' + record_folder + '/monitoring/*_{{ ds_nodash }}.* ' + monitoring_output_folder,
    dag=forecast_monitoring
)

check_dc_order.set_upstream(run_monitoring)
check_store_order.set_upstream(run_monitoring)
run_consistency.set_upstream(run_monitoring)
copy_output.set_upstream(check_dc_order)
copy_output.set_upstream(check_store_order)
copy_output.set_upstream(run_consistency)