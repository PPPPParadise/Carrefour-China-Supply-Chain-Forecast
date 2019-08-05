import pandas as pd
import os
import json
import sys
proc_root = os.path.dirname(os.path.realpath(__file__))
os.chdir(proc_root)
sys.path.append(proc_root)

import pickle
import numpy as np
import pyspark
import matplotlib.pyplot as plt
import warnings
import datetime
import time
import csv
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row


os.environ["SPARK_HOME"] = '/opt/cloudera/parcels/CDH-6.1.0-1.cdh6.1.0.p0.770702/lib/spark'
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
warehouse_location = abspath('spark-warehouse')

config = {}
config['database'] = 'temp'

spark = SparkSession \
    .builder \
    .appName("Promo_ROI_test_case") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.num.executors", '10') \
    .config("spark.executor.memory", '15G') \
    .config("spark.executor.cores", '20') \
    .enableHiveSupport() \
    .getOrCreate()

file_name = f"check_null-{time.strftime('%Y%m%d',time.localtime(time.time()))}.txt"

def record_in_txt(file_name,title,sql,df1):
    record_result = open(f'{proc_root}/{file_name}', 'a')
    record_result.write(f'\n\n{title}')
    record_result.write(f'\ntime: {str(datetime.datetime.now())}')
    record_result.write('\nSQL:')
    record_result.write(sql)
    record_result.write('\nResult:')
    record_result.write(f'\n{str(df1)}')
    record_result.close()

def check_table(table_name,database='vartefact'):
    print(f"checking table: {database}.{table_name}")
    sql = f"""describe {database}.{table_name}"""
    sdf = spark.sql(sql)
    cols_name = sdf.toPandas()
    cols_name = cols_name[(~cols_name.col_name.str.contains('#')) & (cols_name.col_name!='data_tye')]
    sql_cols = ''
    list_cols_name = list(set(cols_name['col_name']))
    for col in list_cols_name:
        sql_cols = f"""{sql_cols} count({col}) as {col},"""
    sql_cols = sql_cols[:-1]
    sql = f"""select count(1) as total_num,{sql_cols} from {database}.{table_name}"""
    sdf = spark.sql(sql)
    df = sdf.toPandas()
    table_cols = list(df.columns)
    new_dict = {}
    new_dict['col'] = table_cols
    new_dict['counts'] = []
    for i in new_dict['col']:
        new_dict['counts'].append(df.iloc[0][i])
    new_df = pd.DataFrame(new_dict)
    new_df['proportion'] = new_df['counts']/df.iloc[0]['total_num']
    new_df.sort_values(by=['counts'],inplace=True)
    record_in_txt(file_name,table_name,sql,new_df)
    new_df.to_csv(f"{proc_root}/Test/data/row_number_and_null_value_in_{database}_{table_name}.csv",index=False)
    print(f"{database}.{table_name} checked result: {new_df} ")
    print(f"{database}.{table_name} checked result more detail in Test/data folder")
    return new_df

if __name__ == '__main__':
    df = check_table('m2_item_info',config['database'])
    df = check_table('m2_trxns',config['database'])
    df = check_table('m2_dm_info_per_item_day',config['database'])
    df = check_table('m2_item_sales_info_daily_step1',config['database'])
    