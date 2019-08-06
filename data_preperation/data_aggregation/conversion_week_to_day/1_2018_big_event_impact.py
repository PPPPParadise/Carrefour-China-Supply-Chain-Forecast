import pyspark
import os
import sys
from os.path import expanduser, join, abspath
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from impala.dbapi import connect
import argparse


## example: python3.6 /data/jupyter/ws_dongxue/dongxue_forecast_dataflow/sqls/PRED_TO_DAY/1_2018_big_event_impact.py  -d temp -s 20170101 -e 20190715
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database_name", help="database name")
parser.add_argument("-s", "--start_date", help="start date")
parser.add_argument("-e", "--end_date", help="end date")
parser.add_argument("-c", "--config_folder", help="config folder")
args = parser.parse_args()


if (args.database_name is None) or (args.start_date is None) or (args.end_date is None):
    print('config needed ')
    sys.exit()
database_name = args.database_name
start_date = args.start_date
end_date = args.end_date
config_folder = args.config_folder
os.chdir(config_folder)


def impalaexec(sql,create_table=False):
    """
    execute sql using impala
    """
    print(sql)
    with connect(host='dtla1apps14', port=21050, auth_mechanism='PLAIN', user='CHEXT10211', password='datalake2019', database=database_name) as conn:
        curr = conn.cursor()
        curr.execute(sql)


warehouse_location = abspath('spark-warehouse')
os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

sc = SparkSession.builder \
    .appName("Special Day Weekly") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.num.executors", '10') \
    .config("spark.executor.memory", '15G') \
    .config("spark.executor.cores", '20') \
    .enableHiveSupport() \
    .getOrCreate()

sqlc = SQLContext(sc)

sql_query = f"""
select 
    trxn.item_id,
    trxn.sub_id, 
    trxn.store_code,
    trxn.date_key,
    trxn.daily_sales_sum,
    cal.week_key
from {database_name}.forecast_sprint4_add_dm_to_daily trxn
join ods.dim_calendar cal on
 trxn.date_key >= '{start_date}' and trxn.date_key <'{end_date}'
 and trxn.date_key = cal.date_key
""".replace("\n", " ")

df_trxn_all = sqlc.sql(sql_query)

raw_df = df_trxn_all.filter((df_trxn_all.week_key==201806) | (df_trxn_all.week_key==201807 ))

daily_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code', 'date_key']).agg(F.avg("daily_sales_sum").alias("daily_avg"))

all_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code']).agg(F.avg("daily_sales_sum").alias("all_avg"))

joined_df = daily_df.join(all_df, ['item_id', 'sub_id', 'store_code']) 

joined_df = joined_df.withColumn("impacgt", (joined_df.daily_avg - joined_df.all_avg ) / joined_df.all_avg) 

cny_df = joined_df.withColumn("big_event", lit("CNY")) 

raw_df = df_trxn_all.filter((df_trxn_all.week_key==201809) | (df_trxn_all.week_key==201810 ))

daily_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code', 'date_key']).agg(F.avg("daily_sales_sum").alias("daily_avg"))

all_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code']).agg(F.avg("daily_sales_sum").alias("all_avg"))

joined_df = daily_df.join(all_df, ['item_id', 'sub_id', 'store_code']) 

joined_df = joined_df.withColumn("impacgt", (joined_df.daily_avg - joined_df.all_avg ) / joined_df.all_avg) 

wm_df = joined_df.withColumn("big_event", lit("WM")) 

raw_df = df_trxn_all.filter((df_trxn_all.week_key==201845) | (df_trxn_all.week_key==201846 ))

daily_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code', 'date_key']).agg(F.avg("daily_sales_sum").alias("daily_avg"))

all_df = raw_df.groupBy(['item_id', 'sub_id', 'store_code']).agg(F.avg("daily_sales_sum").alias("all_avg"))

joined_df = daily_df.join(all_df, ['item_id', 'sub_id', 'store_code']) 

joined_df = joined_df.withColumn("impacgt", (joined_df.daily_avg - joined_df.all_avg ) / joined_df.all_avg) 

sd_df = joined_df.withColumn("big_event", lit("SD")) 

res_df = cny_df.union(wm_df).union(sd_df)

res_df.write.mode("overwrite").saveAsTable(f"{database_name}.2018_big_event_impact")

sqlc.sql(f"select count(*) from {database_name}.2018_big_event_impact").show()

os.system(f"hadoop fs -rm 2_0forecast_big_events.csv")
os.system(f"hadoop fs -put -f 2_0forecast_big_events.csv")
spark_df = sqlc.read.format('com.databricks.spark.csv').options(header='true').load(f"2_0forecast_big_events.csv")
spark_df.write.mode('overwrite').saveAsTable(f"{database_name}.forecast_big_events")
sql = f""" invalidate metadata {database_name}.forecast_big_events """
impalaexec(sql)
print('csv saved in the table')

os.system(f"hadoop fs -rm 3_0forecast_dm_pattern.csv")
os.system(f"hadoop fs -put -f 3_0forecast_dm_pattern.csv")
spark_df = sqlc.read.format('com.databricks.spark.csv').options(header='true').load(f"3_0forecast_dm_pattern.csv")
spark_df.write.mode('overwrite').saveAsTable(f"{database_name}.forecast_dm_pattern")
sql = f""" invalidate metadata {database_name}.forecast_dm_pattern """
impalaexec(sql)
print('csv saved in the table')


sc.stop()