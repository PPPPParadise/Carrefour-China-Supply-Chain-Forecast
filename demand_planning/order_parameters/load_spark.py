import pyspark
import os
from os.path import expanduser, join, abspath
from pyspark import SparkContext
from pyspark.sql import SparkSession

def load_spark(app_name):
    warehouse_location = abspath('spark-warehouse')
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

    sc = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", '16g') \
        .config("spark.executor.memory", '8g') \
        .config("spark.cores.max", '32') \
        .config("hive.exec.compress.output", 'false') \
        .enableHiveSupport() \
        .getOrCreate()
    
    sc.conf.set("spark.executor.memory", "8g")
    
    return sc