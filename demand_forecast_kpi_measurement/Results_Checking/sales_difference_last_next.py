import calendar
import datetime
import os
import sys
from datetime import timedelta
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from openpyxl import Workbook
from os.path import abspath
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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

output_path = "/data/jupyter/ws_shawn/notebook/Forecast/item_order"
run_date = datetime.datetime.strptime("20190826",'%Y%m%d')
last1week_start_date = (run_date - timedelta(weeks=1)).strftime("%Y%m%d")
last3week_start_date = (run_date - timedelta(weeks=3)).strftime("%Y%m%d")
next1week_start_date = (run_date + timedelta(weeks=1)).strftime("%Y%m%d")
next3week_start_date = (run_date + timedelta(weeks=3)).strftime("%Y%m%d")
run_date_str = run_date.strftime("%Y%m%d")


def calculate_difference(date,before_date,next_date):

    print ("start:{}".format(datetime.datetime.now()))

    df_real_sales_sql = f"""
        with item_list as (
            select
                dept_code,
                item_code,
                sub_code,
                store_code,
                date_key,
                daily_sales_sum
            from vartefact.forecast_sprint4_add_dm_to_daily
            where date_key >= {before_date}
            and date_key <= {date}
        ),
        item_list_adding_week as
        (
            select
                a.*,
                b.week_key
            from item_list a
            left join vartefact.lddb_calendar b
            on a.date_key = b.date_key
        ),
        item_week_avg as
        (
            select
                dept_code,
                item_code,
                sub_code,
                week_key,
                sum(daily_sales_sum) as sales_week
            from item_list_adding_week
            group by dept_code,item_code,sub_code,week_key
        )
        select
            dept_code,
            item_code,
            sub_code,
            avg(sales_week) as sales_avg_real
        from item_week_avg
        group by dept_code,item_code,sub_code
    """

    df_real_sales = sqlc.sql(df_real_sales_sql).toPandas()

    print ("Step 1 Ends:{}".format(datetime.datetime.now()))

    df_predict_sql = f"""
        with item_predict_sales as
        (
            select
                item_id,
                sub_id,
                week_key,
                sum(daily_sales_pred) as sales_predict
            from vartefact.forecast_regular_results_week_to_day_original_pred_all
            where date_key >= {date}
            and date_key <= {next_date}
            group by item_id,sub_id,week_key
        ),
            item_id_mapping as
        (
            select
                distinct
                dept_code,
                item_code,
                sub_code,
                item_id,
                sub_id
            from fds.p4cm_daily_stock
            where item_id in (select item_id from item_predict_sales)
            and date_key >= {date}
            and date_key <= {next_date}
        ),
            item_code_predict as
        (
            select
                b.dept_code as dept_code,
                b.item_code as item_code,
                b.sub_code as sub_code,
                a.week_key as week_key,
                a.sales_predict as sales_predict
            from item_predict_sales a
            left join item_id_mapping b
            on a.item_id = b.item_id
            and a.sub_id = b.sub_id
        )
            select
                dept_code,
                item_code,
                sub_code,
                avg(sales_predict) as sales_avg_predict
            from item_code_predict
            group by dept_code,item_code,sub_code
    """

    df_predict = sqlc.sql(df_predict_sql).toPandas()

    print ("Step 2 Ends:{}".format(datetime.datetime.now()))

    ### 3. Merge table

    df_all = pd.merge(df_predict,df_real_sales,how ='left',on = ['dept_code','item_code','sub_code'])

    df_all['sales_avg_real'] = df_all['sales_avg_real'].apply(float)

    df_all['sales_avg_predict'] = df_all['sales_avg_predict'].apply(float)

    df_all['diff'] = np.abs(df_all['sales_avg_predict'] - df_all['sales_avg_real'])

    df_all.sort_values(by = 'diff',ascending = False).reset_index(drop = True).\
    to_csv(f'{output_path}/item_list_difference_between_from_{before_date}_to_{run_date_str}_and_from_{run_date_str}_to_{next_date}.csv',index = False)

    plt.figure(figsize=(16,9))
    plt.hist(df_all[~df_all['diff'].isnull()]['diff'],bins = 100,color = '#2AB6BF')
    plt.title(f'Distribution for sales difference between from {before_date} to {run_date_str} and from {run_date_str} to {next_date}',fontsize = 15)
    plt.xlabel('Count',fontsize = 15)
    plt.ylabel('Difference',fontsize = 15)
    plt.savefig(f'{output_path}/sales_difference_between_from_{before_date}_to_{run_date_str}_and_from_{run_date_str}_to_{next_date}.png')
    plt.show()

    print ("Finally Ends:{}".format(datetime.datetime.now()))


df_next_one_last_three = calculate_difference(run_date_str,last3week_start_date,next1week_start_date)

df_next_three_last_three = calculate_difference(run_date_str,last3week_start_date,next3week_start_date)
