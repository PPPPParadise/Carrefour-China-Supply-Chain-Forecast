# -*- coding: utf-8 -*-

import datetime
import os

from download_data import download_data
from weekly_model_preprocessing import preprocess
from weekly_model_train import run_model

proc_root = os.path.dirname(os.path.realpath(__file__))

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import lpad
from impala.dbapi import connect
import argparse

os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'
warehouse_location = abspath('spark-warehouse')

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database_name", help="database name")
parser.add_argument("-f", "--local_folder", help="local folder")
parser.add_argument("-s", "--date_stop_train", help="date stop train")
args = parser.parse_args()

config = {}
config['database'] = args.database_name
config['local_folder'] = args.local_folder
config['date_stop_train'] = args.date_stop_train
print(config)
if (args.database_name is None) or (args.local_folder is None) or (args.date_stop_train is None):
    print('config needed ')
    sys.exit()


def impalaexec(sql, create_table=False):
    """
    execute sql using impala
    """
    print(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019',
                 database=config['database']) as conn:
        curr = conn.cursor()
        curr.execute(sql)


def main():
    # Download data
    now = datetime.datetime.now().strftime("%m-%d-%H-%M-%S")
    folder = config['local_folder']
    os.system(f"rm -r {folder}")

    # Weekly dataset
    big_table_name = f"{config['database']}.forecast_sprint3_v10_flag_sprint4"
    file_name = 'test_to_delete_' + now
    download_data(folder=folder, big_table_name=big_table_name, file_name=file_name)

    # Preprocess the datase
    big_table = file_name + '.csv'
    sql_table = big_table_name
    target_value = 'sales_qty_sum'
    dataset_name = 'dataset_test_weekly'
    preprocess(folder=folder, big_table=big_table, sql_table=sql_table,
               target_value=target_value, dataset_name=dataset_name)

    # Train the model
    desc = 'weekly_test'
    data_set1 = dataset_name + '_part1.pkl'
    data_set2 = dataset_name + '_part2.pkl'

    date_stop_train = config['date_stop_train']
    os.system(f"cp {proc_root}/calendar.pkl {config['local_folder']}")

    run_model(folder=folder, data_set1=data_set1, data_set2=data_set2, futur_prediction=True,
              date_stop_train=date_stop_train)
    # save csv as table
    print('saving csv as table')
    file_list = os.listdir(config['local_folder'])

    for f in file_list:
        if 'weekly_model_training_for_futur_predictions' in f:
            result_file_name = f
    result_csv_path = f"{config['local_folder']}/{result_file_name}/resulst_forecast_10w_on_the_fututre.csv"
    os.system(f"hadoop fs -rm resulst_forecast_10w_on_the_fututre.csv")
    os.system(f"hadoop fs -put -f {result_csv_path}")

    spark = SparkSession \
        .builder \
        .appName("Forecast_saveastable") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.num.executors", '10') \
        .config("spark.executor.memory", '15G') \
        .config("spark.executor.cores", '20') \
        .enableHiveSupport() \
        .getOrCreate()

    sqlContext = SQLContext(spark)

    spark_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(
        f"resulst_forecast_10w_on_the_fututre.csv")
    split_col = pyspark.sql.functions.split(spark_df['full_item'], '_')
    spark_df = spark_df.withColumn('item_id', split_col.getItem(0))
    spark_df = spark_df.withColumn('sub_id', split_col.getItem(1))
    spark_df = spark_df.withColumn("item_id", spark_df["item_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("sub_id", spark_df["sub_id"].cast(IntegerType()))
    spark_df = spark_df.withColumn("week", spark_df["week"].cast(IntegerType()))
    spark_df = spark_df.withColumn("train_mape_score", spark_df["train_mape_score"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales", spark_df["predict_sales"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales_error_squared",
                                   spark_df["predict_sales_error_squared"].cast(FloatType()))
    spark_df = spark_df.withColumn("predict_sales_max_confidence_interval",
                                   spark_df["predict_sales_max_confidence_interval"].cast(FloatType()))
    spark_df = spark_df.withColumn("order_prediction", spark_df["order_prediction"].cast(FloatType()))
    spark_df = spark_df.withColumn("store_code", lpad(spark_df['store_code'], 3, '0'))
    spark_df = spark_df.withColumnRenamed('week', 'week_key')
    spark_df = spark_df.withColumnRenamed('predict_sales', 'sales_prediction')
    spark_df = spark_df.withColumnRenamed('predict_sales_max_confidence_interval', 'max_confidence_interval')
    spark_df.write.mode('overwrite').saveAsTable(f"{config['database']}.result_forecast_10w_on_the_fututre")
    spark.stop()

    sql = f""" invalidate metadata {config['database']}.result_forecast_10w_on_the_fututre """
    impalaexec(sql)
    print('csv saved in the table')


if __name__ == '__main__':
    main()
