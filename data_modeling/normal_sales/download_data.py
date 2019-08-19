# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.6
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import pandas as pd
import os
import pickle
import numpy as np
import pyspark
import matplotlib.pyplot as plt
import warnings
import datetime
import csv
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import Row


def download_data(folder, big_table_name, file_name, spark_session_name='download_data_forecast'):
    """Download the data from the datalake to the local server

    Arguments:
        folder {[string]} -- [Name of the folder to create]
        big_table_name {[string]} -- [Name of the big table]
        file_name {[string]} -- [Name of the file to save on the server]
        spark_session_name {[string]} -- [name of the sparksession] (default: {download_data_forecast})

    Returns:
        [] -- []
    """

    try:
        os.mkdir(folder)
        print("Directory created")
    except FileExistsError:
        raise ("Directory already exists, please change directory name")

    warehouse_location = abspath('spark-warehouse')

    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
    os.environ["SPARK_HOME"] = '/opt/cloudera/parcels/CDH-6.1.0-1.cdh6.1.0.p0.770702/lib/spark'

    spark = SparkSession \
        .builder \
        .appName(spark_session_name) \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.num.executors", '15') \
        .config("spark.executor.memory", '20G') \
        .config("spark.executor.cores", '25') \
        .enableHiveSupport() \
        .getOrCreate()

    # # SQL to extract data (change SQL script)
    sql_query = f"""
    select *
    from {big_table_name}
    """
    # where item_id = '16574'
    # where item_id in (select distinct item_id from {big_table_name} limit 5)

    # # Download script (no need to change)
    os.system(f'hadoop fs -rm -r {file_name}')
    os.system(f'hadoop fs -rm -r {file_name}.csv')
    print('started')
    df_trxn_all = spark.sql(sql_query)
    df_trxn_all.coalesce(1).write.mode('overwrite')\
        .format('com.databricks.spark.csv').option("header", "true").save(file_name)
    print('ended')
    spark.stop()
    # +
    print('started')
    os.system(f'hadoop fs -ls {file_name} > out_put_forecast_temp.txt')

    for r in open('out_put_forecast_temp.txt'):
        result = r
    print(result)

    file_name_on_hdfs = result.split(' ')[-1].strip()
    os.system(
        f'hadoop fs -text {file_name_on_hdfs} | hadoop fs -put - {file_name}.csv   ')
    os.system(f'hadoop fs -get -f {file_name}.csv ')
    # -
    print('Download successfull!')
    print(file_name)

    try:
        os.rename(file_name+'.csv', folder+file_name+'.csv')
        print('Dataset moved to directory')
    except:
        print('no downloaded file to move')
    os.system(f'rm out_put_forecast_temp.txt')


if __name__ == '__main__':
    """[To download data from the datalake to locally]
    """
    big_table_name = 'vartefact.forecast_sprint4_add_dm_to_daily'
    file_name = 'test_to_delete'
    folder = 'test_folder/'

    download_data(folder=folder, big_table_name=big_table_name, file_name=file_name)
