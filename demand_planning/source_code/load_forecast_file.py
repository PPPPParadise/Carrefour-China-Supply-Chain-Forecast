# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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

# +
import datetime
import os
from datetime import timedelta
from os.path import abspath

import pandas as pd
import numpy as np
from openpyxl import Workbook
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# + {"endofcell": "--"}
warehouse_location = abspath('spark-warehouse')

os.environ["PYSPARK_SUBMIT_ARGS"] = '--jars /data/jupyter/kudu-spark2_2.11-1.8.0.jar pyspark-shell'

spark = SparkSession.builder \
    .appName("Load order forecast file to database") \
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
# -
# --

def extract_weekly_forecast(excel_input, week_no, run_date, con_holding):
    week_add = week_no - 1
    
    week_start_date = (run_date + timedelta(weeks=week_add)).strftime("%Y%m%d")
    
    df = excel_input.loc[:, ['Department_code', 'Item_code', 
                  'Sub_code', 'Item_desc_chn', 
                  f'Week{week_no}_{week_start_date}_Permanent_Box', 
                  f'Week{week_no}_{week_start_date}_DM_Box']]
    
    df.columns = ['dept_code','item_code','sub_code', 'item_desc_chn','order_qty','dm_order_qty']
    
    df['con_holding'] = con_holding
    df['week_start_day'] = week_start_date
    
    return df


def get_res_df(excel_input, run_date, con_holding):
    
    result = pd.concat([extract_weekly_forecast(excel_input, 1, run_date, con_holding), 
                        extract_weekly_forecast(excel_input, 2, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 3, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 4, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 5, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 6, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 7, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 8, run_date, con_holding),
                        extract_weekly_forecast(excel_input, 9, run_date, con_holding)], ignore_index=True)
    
    return result


# +
date_str = '20190909'

run_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()

output_path = "/data/jupyter/Carrefour-China-Supply-Chain-Forecast/output/forecast_files/"

run_date_str = run_date.strftime("%Y%m%d")

# +
con_holding = "693"

file_name = 'Carrefour_Order_Forecast_DC_level_{0}_{1}.xlsx'.format(con_holding, run_date_str)
    
pg_excel_input = pd.read_excel(output_path + file_name, 'Sheet', header=0, dtype=str).fillna("0")

pg_df = get_res_df(pg_excel_input, run_date, con_holding)

con_holding = "002"

file_name = 'Carrefour_Order_Forecast_DC_level_{0}_{1}.xlsx'.format(con_holding, run_date_str)
    
ns_excel_input = pd.read_excel(output_path + file_name, 'Sheet', header=0, dtype=str).fillna("0")

ns_df = get_res_df(ns_excel_input, run_date, con_holding)

con_holding = "700"

file_name = 'Carrefour_Order_Forecast_DC_level_{0}_{1}.xlsx'.format(con_holding, run_date_str)
    
un_excel_input = pd.read_excel(output_path + file_name, 'Sheet', header=0, dtype=str).fillna("0")

un_df = get_res_df(un_excel_input, run_date, con_holding)

# +
result_df = pd.concat([pg_df, ns_df, un_df], ignore_index=True)

result_df = result_df.replace(np.NaN, '0')
# -

sqlc.createDataFrame(result_df).createOrReplaceTempView("weekly_forecast_df")

dm_dc_sql = \
    """
    INSERT OVERWRITE TABLE vartefact.forecast_weekly_forecast_file
    PARTITION (run_date)
    SELECT 
        week_start_day,
        con_holding,
        dept_code,
        item_code,
        sub_code,
        item_desc_chn,
        cast(order_qty as int) as order_qty,
        cast(dm_order_qty as int) as dm_order_qty,
        {0} as run_date
    FROM weekly_forecast_df
    """.replace("\n", " ").format(run_date_str)

sqlc.sql(dm_dc_sql)

sqlc.sql("refresh table vartefact.forecast_weekly_forecast_file")

sc.stop()


