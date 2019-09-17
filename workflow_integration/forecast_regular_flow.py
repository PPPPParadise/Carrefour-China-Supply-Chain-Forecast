# -*- coding: utf-8 -*-
import sys
import os
from os.path import expanduser, join, abspath
import traceback

import datetime
import time
import csv
import re
from random import shuffle
import pandas as pd
import pickle
import numpy as np
import pyspark
from pyspark.sql import SQLContext 
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
from os.path import expanduser, join, abspath
import functools
from impala.dbapi import connect
import multiprocessing as mp
from multiprocessing import Queue
import pprint
from dateutil import parser

# -- Define function for log printing and saving
# -- From:
# -- To: Function:module_logger.error(),
#        Function:print()
# -- Usage: Log printing and saving
# -- Note:

import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from openpyxl import Workbook

project_folder = Variable.get("project_folder")
order_output_folder = Variable.get("order_output_folder")
store_order_file = Variable.get("store_order_file_name")


# airflow backfill forecast_normal_dm_flow -s 2019-08-19 -e 2019-08-21

############################### Config ##########################
default_args = {
    'owner': 'Carrefour',
    'start_date': datetime.datetime(2019, 8, 13),
    'email': ['vincent.lin@artefact.com'],
    'depends_on_past': False, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'end_date': datetime.datetime(2030, 1, 1),
}

dag = DAG('forecast_regular_flow',
          schedule_interval='0 2 * * 2 0',
          default_args=default_args, catchup=False)

config = {}
config['database'] = 'vartefact'
config['parent_path'] = "/data/jupyter/Carrefour-China-Supply-Chain-Forecast"
config['config_data_path'] = config['parent_path'] + "/config/input_config_data" 
config['starting_date'] = 20170101
###############################  End  ##########################


os.chdir(config['parent_path'])
sys.path.append(config['parent_path'])


def impalaexec(sql,create_table=False):
   """
   execute sql using impala
   """
   print(sql)
   while True:
      try:
            with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database']) as conn:
               curr = conn.cursor()
               curr.execute(sql)
            break
      except:
            print(sys.exc_info())
            if create_table != False:
               sql_drop = f'''
               drop table if exists {create_table}
               '''
               impalaexec(sql_drop)
            time.sleep(300)



def hiveexec(sql,create_table=False):
   """
   execute sql using hive
   """
   print(sql)
   while True:
      try:
            '''
            execute sql by impala
            '''
            with connect(host='dtla1apps11', port=10000, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=config['database']) as conn:
               curr = conn.cursor()
               curr.execute(sql)
            break
      except:
            print(sys.exc_info())
            if create_table != False:
               sql_drop = f'''
               drop table if exists {create_table}
               '''
               impalaexec(sql_drop)
            time.sleep(300)


def execute_impala_by_sql_file(table_name,file_path,set_timeperiod=False,database='config',dropfirst=True,**kwargs):
   """
   execute_impala_by_sql_file
   """
   #set database
   if database == 'config':
      database = config['database']
   #drop table if dropfirst is true
   if dropfirst:
      sql = f""" drop table if exists {database}.{table_name} """
      impalaexec(sql)
      sql = f""" drop view if exists {database}.{table_name} """
      impalaexec(sql)
   #read SQL from file
   with open(f'{file_path}') as f:
      sql = f.read()
   #pass the time parameter if set_timeperiod is true
   if set_timeperiod:
      starting_date = config['starting_date']
      print(f"get starting_date {starting_date}") 
      ############################ airflow only trigger after interval is passed ############################
      delta = datetime.timedelta(days = 6)
      ending_date_withline = str((parser.parse(kwargs.get('ds'))+delta).date())
      ending_date = ending_date_withline.replace('-','')
      print(f"get ending_date {ending_date}") 
      sql = sql.format(database=database,starting_date=starting_date,ending_date=ending_date,ending_date_withline=ending_date_withline)
   else:
      sql = sql.format(database=database)
   # execute the SQL
   database_table_name = f"{database}.{table_name}"
   if '_all' not in database_table_name:
      impalaexec(sql,database_table_name)
   else:
      impalaexec(sql)
   # update the table
   sql = f""" INVALIDATE METADATA {database}.{table_name} """
   impalaexec(sql)


def execute_hive_by_sql_file(table_name,file_path,set_timeperiod=False,database='config',dropfirst=True,**kwargs):
   """
   execute_hive_by_sql_file
   """
   #set database
   if database == 'config':
      database = config['database']
   #drop table if dropfirst is true
   if dropfirst:
      sql = f""" drop table if exists {database}.{table_name} """
      hiveexec(sql)
      sql = f""" drop view if exists {database}.{table_name} """
      hiveexec(sql)
   #read SQL from file
   with open(f'{file_path}') as f:
      sql = f.read()
   #pass the time parameter if set_timeperiod is true
   if set_timeperiod:
      # delta = datetime.timedelta(days = 7)
      starting_date = config['starting_date']
      print(f"get starting_date {starting_date}") 
      ############################ airflow only trigger after interval is passed ############################
      delta = datetime.timedelta(days = 6)
      ending_date_withline = str((parser.parse(kwargs.get('ds'))+delta).date())
      ending_date = ending_date_withline.replace('-','')
      print(f"get ending_date {ending_date}") 
      sql = sql.format(database=database,starting_date=starting_date,ending_date=ending_date,ending_date_withline=ending_date_withline)
   else:
      sql = sql.format(database=database)
   # execute the SQL
   database_table_name = f"{database}.{table_name}"
   if '_all' not in database_table_name:
      hiveexec(sql,database_table_name)
   else:
      hiveexec(sql)
   # update the table
   sql = f""" INVALIDATE METADATA {database}.{table_name} """
   impalaexec(sql)
    
    
def print_context(ds, **kwargs):
    pprint.pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

def get_spark():
   print('Trying to get spark connection...')
   warehouse_location = os.path.abspath('spark-warehouse')
   spark = (SparkSession \
      .builder \
      .appName("Forecast") \
      .config("spark.sql.warehouse.dir", warehouse_location) \
      .config("spark.num.executors", '15') \
      .config("spark.executor.memory", '20G') \
      .config("spark.executor.cores", '25') \
      .enableHiveSupport() \
      .getOrCreate()
   ) 
   print('Spark connection created!')
   return spark

run_this = PythonOperator(
   task_id='Start',
   provide_context=True,
   python_callable=print_context,
   dag=dag,
)

#1
# execute_impala_by_sql_file('forecast_store_code_scope_sprint4',\
#                            './sqls/1.forecast_store_code_scope_sprint4.sql')
step1 = PythonOperator(task_id="step1",
                              python_callable=execute_impala_by_sql_file,
                              op_kwargs={'table_name': "forecast_store_code_scope_sprint4",
                                 'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/1.forecast_store_code_scope_sprint4.sql'},
                              dag=dag)
step1.set_upstream(run_this)

#2
# execute_impala_by_sql_file('forecast_itemid_list_threebrands_sprint4',\
#                            './sqls/2.forecast_itemid_list_threebrands_sprint4.sql')
step2 = PythonOperator(task_id="step2",
                              python_callable=execute_impala_by_sql_file,
                              provide_context=True,
                              op_kwargs={'table_name': "forecast_itemid_list_threebrands_sprint4",
                                 'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/2.forecast_itemid_list_threebrands_sprint4.sql',
                                 'set_timeperiod':True},
                              dag=dag)
step2.set_upstream(step1)

#3
# execute_impala_by_sql_file('forecast_item_id_family_codes_sprint4',\
#                            './sqls/3.vartefact.forecast_item_id_family_codes_sprint4.sql')
step3 = PythonOperator(task_id="step3",
                        python_callable=execute_impala_by_sql_file,
                        op_kwargs={'table_name': "forecast_item_id_family_codes_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/3.vartefact.forecast_item_id_family_codes_sprint4.sql'},
                        dag=dag)
step3.set_upstream(step2)

#4
# execute_impala_by_sql_file('lastest_active_status',\
#                            './sqls/4.lastest_active_status.sql')
step4 = PythonOperator(task_id="step4",
                        python_callable=execute_impala_by_sql_file,
                        provide_context=True,
                        op_kwargs={'table_name': "lastest_active_status",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/4.lastest_active_status.sql',
                           'set_timeperiod':True},
                        dag=dag)
step4.set_upstream(step3)

#5
# execute_impala_by_sql_file('forecast_dm_plans_sprint4',\
#                            './sqls/5.forecast_dm_plans_sprint4.sql')
step5 = PythonOperator(task_id="step5",
                        python_callable=execute_impala_by_sql_file,
                        provide_context=True,
                        op_kwargs={'table_name': "forecast_dm_plans_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/5.forecast_dm_plans_sprint4.sql',
                           'set_timeperiod':True},
                        dag=dag)
step5.set_upstream(step4)

#6
# execute_impala_by_sql_file('forecast_add_future_dms_sprint4',\
#                            './sqls/6.forecast_add_future_dms_sprint4.sql')
step6 = PythonOperator(task_id="step6",
                        python_callable=execute_impala_by_sql_file,
                        provide_context=True,
                        op_kwargs={'table_name': "forecast_add_future_dms_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/6.forecast_add_future_dms_sprint4.sql',
                           'set_timeperiod':True},
                        dag=dag)
step6.set_upstream(step5)

#7
# execute_impala_by_sql_file('forecast_next_dm_sprint4',\
#                            './sqls/7.forecast_next_dm_sprint4.sql')
step7 = PythonOperator(task_id="step7",
                        python_callable=execute_impala_by_sql_file,
                        op_kwargs={'table_name': "forecast_next_dm_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/7.forecast_next_dm_sprint4.sql'},
                        dag=dag)
step7.set_upstream(step6)

#8
# execute_impala_by_sql_file('forecast_trxn_v7_sprint4',\
#                            './sqls/8.forecast_trxn_v7_sprint4.sql')
step8 = PythonOperator(task_id="step8",
                        python_callable=execute_impala_by_sql_file,
                        provide_context=True,
                        op_kwargs={'table_name': "forecast_trxn_v7_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/8.forecast_trxn_v7_sprint4.sql',
                           'set_timeperiod':True},
                        dag=dag)
step8.set_upstream(step7)

#9.0
# execute_impala_by_sql_file('forecast_trxn_v7_full_item_id_sprint4',\
#                            './sqls/9.0forecast_trxn_v7_full_item_id_sprint4.sql')
step9 = PythonOperator(task_id="step9",
                        python_callable=execute_impala_by_sql_file,
                        op_kwargs={'table_name': "forecast_trxn_v7_full_item_id_sprint4",
                           'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.0forecast_trxn_v7_full_item_id_sprint4.sql'},
                        dag=dag)
step9.set_upstream(step8)

#9.1
# 9.1  Scala script
# op_kwargs={'table_name': "forecast_trxn_v7_full_item_id_sprint4_group_id_new",
def step9_1_execute_scala():
   #  os.system(f"""spark-submit --class --master yarn --num-executors 8 {config['parent_path']}/sqls/BpTrxnGroup-assembly-1.0.jar""")
   checking_script = os.system(f"""spark-submit --class "carrefour.forecast.process.BpTrxnGroup" --master yarn --num-executors 8 --executor-memory 8G {config['parent_path']}/data_preperation/data_aggregation/regular_item/bptrxngroup_2.11-1.0.jar {config['database']} forecast_trxn_v7_full_item_id_sprint4 forecast_trxn_v7_full_item_id_sprint4_group_id_new""")
   if checking_script != 0:
      print("model tranining failed")
      raise ValueError
   sql = f""" INVALIDATE METADATA {config['database']}.forecast_trxn_v7_full_item_id_sprint4_group_id_new """
   impalaexec(sql)

step9_1 = PythonOperator(task_id="step9_1",
                           python_callable=step9_1_execute_scala,
                           dag=dag)
step9_1.set_upstream(step9)

#9.2
# execute_hive_by_sql_file('art_filter_non_promo',\
#                            './sqls/9.2art_filter_non_promo.sql')
step9_2 = PythonOperator(task_id="step9_2",
                           python_callable=execute_hive_by_sql_file,
                           op_kwargs={'table_name': "art_filter_non_promo",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.2art_filter_non_promo.sql'},
                           dag=dag)
step9_2.set_upstream(step9_1)

#9.3
# execute_hive_by_sql_file('art_filter_promo',\
#                            './sqls/9.3art_filter_promo.sql')
step9_3 = PythonOperator(task_id="step9_3",
                           python_callable=execute_hive_by_sql_file,
                           op_kwargs={'table_name': "art_filter_promo",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.3art_filter_promo.sql'},
                           dag=dag)
step9_3.set_upstream(step9_2)

#9.4
# execute_impala_by_sql_file('grouped_to_be_shipment',\
#                            './sqls/9.4grouped_to_be_shipment.sql')
step9_4 = PythonOperator(task_id="step9_4",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "grouped_to_be_shipment",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.4grouped_to_be_shipment.sql'},
                           dag=dag)
step9_4.set_upstream(step9_3)

#9.5
# execute_impala_by_sql_file('p4cm_item_map_complete',\
#                            './sqls/9.5p4cm_item_map_complete.sql')
step9_5 = PythonOperator(task_id="step9_5",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "p4cm_item_map_complete",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.5p4cm_item_map_complete.sql',
                              'set_timeperiod':True},
                           dag=dag)
step9_5.set_upstream(step9_4)

#9.6
# execute_impala_by_sql_file('shipment_scope_map_corrected',\
#                            './sqls/9.6shipment_scope_map_corrected.sql')
step9_6 = PythonOperator(task_id="step9_6",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "shipment_scope_map_corrected",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.6shipment_scope_map_corrected.sql',
                              'set_timeperiod':True},
                           dag=dag)
step9_6.set_upstream(step9_5)

#9.7 是一个python文件 
# op_kwargs={'table_name': "grouped_to_be_shipment_groupped",
def step9_7_execute_python():
   checking_script = os.system(f"""python3.6 ./data_preperation/data_aggregation/regular_item/9.7grouped_to_be_shipment_groupped_0729.py -d {config['database']}""")
   if checking_script != 0:
      print("model tranining failed")
      raise ValueError
step9_7 = PythonOperator(task_id="step9_7",
                           python_callable=step9_7_execute_python,
                           dag=dag)
step9_7.set_upstream(step9_6)

#9.8
# execute_impala_by_sql_file('forecast_item_store_perc_flagged',\
#                            './sqls/9.8forecast_item_store_perc_flagged.sql')
step9_8 = PythonOperator(task_id="step9_8",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_item_store_perc_flagged",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.8forecast_item_store_perc_flagged.sql'},
                           dag=dag)
step9_8.set_upstream(step9_7)
                           
#9.9
# execute_impala_by_sql_file('forecast_trxn_flag_v1_sprint4',\
#                            './sqls/9.9forecast_trxn_flag_v1_sprint4.sql')
step9_9 = PythonOperator(task_id="step9_9",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_trxn_flag_v1_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/9.9forecast_trxn_flag_v1_sprint4.sql'},
                           dag=dag)
step9_9.set_upstream(step9_8)

#10
# execute_impala_by_sql_file('forecast_sprint4_full_date_daily_sales',\
#                            './sqls/10.forecast_sprint4_full_date_daily_sales.sql')
step10 = PythonOperator(task_id="step10",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_sprint4_full_date_daily_sales",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/10.forecast_sprint4_full_date_daily_sales.sql',
                              'set_timeperiod':True},
                           dag=dag)
step10.set_upstream(step9_9)

#11
# execute_impala_by_sql_file('forecast_sprint4_out_of_stock_median',\
#                            './sqls/11.out_of_stock_median_final.sql')
step11 = PythonOperator(task_id="step11",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_sprint4_out_of_stock_median",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/11.out_of_stock_median_final.sql',
                              'set_timeperiod':True},
                           dag=dag)
step11.set_upstream(step10)

#12.1
# execute_impala_by_sql_file('forecast_sprint4_trxn_to_day',\
#                            '12.1forecast_sprint4_trxn_to_day.sql')
step12_1 = PythonOperator(task_id="step12_1",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint4_trxn_to_day",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.1forecast_sprint4_trxn_to_day.sql'},
                           dag=dag)
step12_1.set_upstream(step11)

#12.2
# execute_impala_by_sql_file('forecast_sprint4_daily_future_dms',\
#                            './sqls/12.2forecast_sprint4_daily_future_dms.sql')
step12_2 = PythonOperator(task_id="step12_2",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint4_daily_future_dms",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.2forecast_sprint4_daily_future_dms.sql'},
                           dag=dag)
step12_2.set_upstream(step12_1)

#12.3
# execute_impala_by_sql_file('forecast_sprint4_daily_next_dms',\
#                            './sqls/12.3forecast_sprint4_daily_next_dms.sql')
step12_3 = PythonOperator(task_id="step12_3",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint4_daily_next_dm",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.3forecast_sprint4_daily_next_dms.sql'},
                           dag=dag)
step12_3.set_upstream(step12_2)

#12.4
# execute_impala_by_sql_file('forecast_sprint4_add_dm_to_daily',\
#                            './sqls/12.4forecast_sprint4_add_dm_to_daily.sql')
step12_4 = PythonOperator(task_id="step12_4",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint4_add_dm_to_daily",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.4forecast_sprint4_add_dm_to_daily.sql'},
                           dag=dag)
step12_4.set_upstream(step12_3)

#12.5
# execute_impala_by_sql_file('forecast_sprint4_day_to_week',\
#                            './sqls/12.5forecast_sprint4_day_to_week.sql')
step12_5 = PythonOperator(task_id="step12_5",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_sprint4_day_to_week",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.5forecast_sprint4_day_to_week.sql',
                              'set_timeperiod':True},
                           dag=dag)
step12_5.set_upstream(step12_4)

# #12.6
# execute_impala_by_sql_file('forecast_spirnt4_day_to_week_test',\
#                            './sqls/12.6forecast_spirnt4_day_to_week_test.sql')
step12_6 = PythonOperator(task_id="step12_6",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint4_day_to_week_test",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/12.6forecast_sprint4_day_to_week_test.sql'},
                           dag=dag)
step12_6.set_upstream(step12_5)

#14
# execute_impala_by_sql_file('forecast_sprint2_festival_ticket_count',\
#                            './sqls/14.forecast_sprint2_festival_ticket_count.sql')
step14 = PythonOperator(task_id="step14",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint2_festival_ticket_count_flag",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/14.forecast_sprint2_festival_ticket_count.sql'},
                           dag=dag)
step14.set_upstream(step12_6)

#15
# execute_impala_by_sql_file('forecast_sprint2_trxn_week_features_flag_sprint4',\
#                            './sqls/15.forecast_sprint2_trxn_week_features_flag_sprint4.sql')
step15 = PythonOperator(task_id="step15",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_sprint2_trxn_week_features_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/15.forecast_sprint2_trxn_week_features_flag_sprint4.sql',
                              'set_timeperiod':True},
                           dag=dag)
step15.set_upstream(step14)

#16
# execute_impala_by_sql_file('forecast_sprint2_final_flag_sprint4',\
#                            './sqls/16.forecast_sprint2_final_flag_sprint4.sql')
step16 = PythonOperator(task_id="step16",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint2_final_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/16.forecast_sprint2_final_flag_sprint4.sql'},
                           dag=dag)
step16.set_upstream(step15)

#17
# execute_impala_by_sql_file('forecast_assortment_full',\
#                            './sqls/17.forecast_assortment_full.sql')
step17 = PythonOperator(task_id="step17",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_assortment_full",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/17.forecast_assortment_full.sql',
                              'set_timeperiod':True},
                           dag=dag)
step17.set_upstream(step16)

#18
# execute_impala_by_sql_file('coupon_mapping',\
#                            './sqls/18.coupon_mapping.sql')
step18 = PythonOperator(task_id="step18",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "coupon_mapping",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/18.coupon_mapping.sql'},
                           dag=dag)
step18.set_upstream(step17)

# #19
# execute_impala_by_sql_file('forecast_sprint3_v3_flag_sprint4',\
#                            './sqls/19.forecast_sprint3_v3_flag_sprint4.sql')
step19 = PythonOperator(task_id="step19",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint3_v3_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/19.forecast_sprint3_v3_flag_sprint4.sql'},
                           dag=dag)
step19.set_upstream(step18)

# #20
# execute_impala_by_sql_file('forecast_sprint3_coupon_item_link_flag_sprint4',\
#                            './sqls/20.forecast_sprint3_coupon_item_link_flag_sprint4.sql')
step20 = PythonOperator(task_id="step20",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_sprint3_coupon_item_link_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/20.forecast_sprint3_coupon_item_link_flag_sprint4.sql',
                              'set_timeperiod':True},
                           dag=dag)
step20.set_upstream(step19)

#21
# execute_impala_by_sql_file('coupon_city_store_union_flag_sprint4',\
#                            './sqls/21.coupon_city_store_union_flag_sprint4.sql')
step21 = PythonOperator(task_id="step21",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "coupon_city_store_union_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/21.coupon_city_store_union_flag_sprint4.sql'},
                           dag=dag)
step21.set_upstream(step20)

#22
# execute_impala_by_sql_file('forecast_sprint3_v5_flag_sprint4',\
#                            './sqls/22.forecast_sprint3_v5_flag_sprint4.sql')
step22 = PythonOperator(task_id="step22",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint3_v5_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/22.forecast_sprint3_v5_flag_sprint4.sql'},
                           dag=dag)
step22.set_upstream(step21)

#23
# execute_impala_by_sql_file('forecast_sprint3_v6_flag_sprint4',\
#                            './sqls/23.forecast_sprint3_v6_flag_sprint4.sql')
step23 = PythonOperator(task_id="step23",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint3_v6_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/23.forecast_sprint3_v6_flag_sprint4.sql'},
                           dag=dag)
step23.set_upstream(step22)

#24
# execute_impala_by_sql_file('forecast_sprint3_v9_flag_sprint4',\
#                            './sqls/24.forecast_sprint3_v9_flag_sprint4.sql')
step24 = PythonOperator(task_id="step24",
                           python_callable=execute_impala_by_sql_file,
                           op_kwargs={'table_name': "forecast_sprint3_v9_flag_sprint4",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/24.forecast_sprint3_v9_flag_sprint4.sql'},
                           dag=dag)
step24.set_upstream(step23)

# #25
# execute_impala_by_sql_file('forecast_out_of_stock_temp',\
#                            './sqls/25.forecast_out_of_stock_temp.sql')
step25 = PythonOperator(task_id="step25",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_out_of_stock_temp",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/25.forecast_out_of_stock_temp.sql',
                              'set_timeperiod':True},
                           dag=dag)
step25.set_upstream(step24)

#26 
# step26 = PythonOperator(task_id="step26",
#                            python_callable=execute_impala_by_sql_file,
#                            op_kwargs={'table_name': "forecast_sprint3_v10_flag_sprint4", 
#                               'file_path':'./sqls/26.forecast_sprint3_v10_flag_sprin4.sql'},
#                            dag=dag)
# step26.set_upstream(step25)
def normal_final_output_table():
   execute_impala_by_sql_file('forecast_sprint3_v10_flag_sprint4',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/26.forecast_sprint3_v10_flag_sprin4.sql')
      # spark.stop()
step26 = PythonOperator(task_id="step26",
                           python_callable=normal_final_output_table,
                           dag=dag)
step26.set_upstream(step25)


# train normal item by python
# op_kwargs={'table_name': "result_forecast_10w_on_the_fututre",  
def step27_model_execute_python(**kwargs):
   ## create summary table if not exists
   execute_impala_by_sql_file('result_forecast_10w_on_the_fututre_all',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/27.result_forecast_10w_on_the_fututre_all-create.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
   delta = datetime.timedelta(days = 5)
   starting_date = str((parser.parse(kwargs.get('ds'))+delta).date())
   checking_script =  os.system(f"""python3.6 {config['parent_path']}/data_modeling/normal_sales/all_included_weekly.py -d {config['database']} -f '{config['parent_path']}/data_modeling/normal_sales/normal_folder_weekly/' -s '{starting_date}' -c '{config['config_data_path']}' """)
   if checking_script != 0:
      print("model tranining failed")
      raise ValueError
   ## insert the new data into the summary table
   execute_impala_by_sql_file('result_forecast_10w_on_the_fututre_all',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/regular_item/27.result_forecast_10w_on_the_fututre_all-insert.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
step27_model = PythonOperator(task_id="step27_model",
                           provide_context=True,
                           python_callable=step27_model_execute_python,
                           dag=dag)
step27_model.set_upstream(step26)

# After getting the results: Split week to day 
# #1 
# # 1_2018_big_event_impact.ipynb 
# op_kwargs={'table_name': "2018_big_event_impact",  
# op_kwargs={'table_name': "forecast_big_events",  
# op_kwargs={'table_name': "forecast_dm_pattern",  
def step_normal_to_day_1_execute_python(**kwargs):
   checking_script = os.system(f""" python3.6 {config['parent_path']}/data_preperation/data_aggregation/conversion_week_to_day/1_2018_big_event_impact.py  -d {config['database']} -s '20180101' -e '20190101' -c '{config['config_data_path']}' """)
   if checking_script != 0:
      print("model tranining failed")
      raise ValueError

step_normal_to_day_1 = PythonOperator(task_id="step_normal_to_day_1",
                           python_callable=step_normal_to_day_1_execute_python,
                           dag=dag)
step_normal_to_day_1.set_upstream(step27_model)

#2 
# execute_impala_by_sql_file('forecast_regular_day',\
#                            '../sqls/PRED_TO_DAY/2_1forecast_regular_day.sql', set_timeperiod=True)
step_normal_to_day_2 = PythonOperator(task_id="step_normal_to_day_2",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_regular_day",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_1forecast_regular_day.sql',
                              'set_timeperiod':True},
                           dag=dag)
step_normal_to_day_2.set_upstream(step_normal_to_day_1)

#3
# execute_impala_by_sql_file('forecast_w2d_good_regular_days',\
#                            '../sqls/PRED_TO_DAY/2_2forecast_w2d_good_regular_days.sql', set_timeperiod=True)
step_normal_to_day_3 = PythonOperator(task_id="step_normal_to_day_3",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_w2d_good_regular_days",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_2forecast_w2d_good_regular_days.sql',
                              'set_timeperiod':True},
                           dag=dag)
step_normal_to_day_3.set_upstream(step_normal_to_day_2)

#4
# execute_impala_by_sql_file('forecast_regular_dayofweek_percentage',\
#                            '../sqls/PRED_TO_DAY/2_3forecast_regular_dayofweek_percentage.sql', set_timeperiod=True)
step_normal_to_day_4 = PythonOperator(task_id="step_normal_to_day_4",
                           python_callable=execute_impala_by_sql_file,
                           provide_context=True,
                           op_kwargs={'table_name': "forecast_regular_dayofweek_percentage",
                              'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_3forecast_regular_dayofweek_percentage.sql',
                              'set_timeperiod':True},
                           dag=dag)
step_normal_to_day_4.set_upstream(step_normal_to_day_3)

#5  
# execute_impala_by_sql_file('forecast_regular_results_week_to_day_original_pred',\
#                            '../sqls/PRED_TO_DAY/2_4forecast_regular_results_week_to_day_original_pred.sql', set_timeperiod=True)
# step_normal_to_day_5 = PythonOperator(task_id="step_normal_to_day_5",
#                            python_callable=execute_impala_by_sql_file,
#                            provide_context=True,
#                            op_kwargs={'table_name': "forecast_regular_results_week_to_day_original_pred",
#                               'file_path':f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_4forecast_regular_results_week_to_day_original_pred.sql',
#                               'set_timeperiod':True},
#                            dag=dag)
# step_normal_to_day_5.set_upstream(step_normal_to_day_4)
# op_kwargs={'table_name': "forecast_regular_results_week_to_day_original_pred_all",
def step_normal_to_day_5_output_table():
   ## create table if not exixts
   execute_impala_by_sql_file('forecast_regular_results_week_to_day_original_pred_all',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_4forecast_regular_results_week_to_day_original_pred_all-create.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
   execute_impala_by_sql_file('forecast_regular_results_week_to_day_original_pred',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_4forecast_regular_results_week_to_day_original_pred.sql',
                              set_timeperiod=False,database='config')
   ## insert into summary table
   execute_impala_by_sql_file('forecast_regular_results_week_to_day_original_pred_all',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/conversion_week_to_day/2_4forecast_regular_results_week_to_day_original_pred_all-insert.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
   ## create view for normal item if 
   execute_impala_by_sql_file('forecast_weekly_normal_view',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/view_for_output/1_weekly_normal_view.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
   execute_impala_by_sql_file('forecast_daily_normal_view',\
                              f'{config["parent_path"]}/data_preperation/data_aggregation/view_for_output/2_daily_normal_view.sql',
                              set_timeperiod=False,database='config',dropfirst=False)
step_normal_to_day_5 = PythonOperator(task_id="step_normal_to_day_5",
                           python_callable=step_normal_to_day_5_output_table,
                           dag=dag)
step_normal_to_day_5.set_upstream(step_normal_to_day_4)
