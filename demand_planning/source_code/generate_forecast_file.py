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

import calendar
import datetime
import os
from datetime import timedelta
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from openpyxl import Workbook
from os.path import abspath
import numpy as np
import pandas as pd

# +
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
# -

run_date = datetime.datetime.strptime("20190902", '%Y%m%d').date()
w1_start_date = run_date
w2_start_date = run_date + timedelta(weeks=1)
w3_start_date = run_date + timedelta(weeks=2)
w4_start_date = run_date + timedelta(weeks=3)
w5_start_date = run_date + timedelta(weeks=4)
w6_start_date = run_date + timedelta(weeks=5)
w7_start_date = run_date + timedelta(weeks=6)
w8_start_date = run_date + timedelta(weeks=7)
w9_start_date = run_date + timedelta(weeks=8)
w10_start_date = run_date + timedelta(weeks=8)

items_sql = """
    SELECT
        dc.holding_code,
        dc.primary_barcode,
        dc.dept_code,
        dc.item_code,
        dc.sub_code,
        dc.item_name_local,
        dc.item_name_english
    FROM vartefact.v_forecast_inscope_dc_item_details dc
""".replace("\n", " ") 

items_df = sqlc.sql(items_sql).toPandas()

# # Cross docking items

# +
xdock_orders_sql = """
    SELECT
        dc.dept_code,
        dc.item_code,
        dc.sub_code,
        wst.date_key week_start_day,
        sum(
            ceil(
                coalesce(ord.order_qty, 0) * (2 - coalesce(sl.service_level, 1)) / dc.qty_per_box
            )
        ) order_qty,
        sum(ceil(coalesce(dm.order_qty, 0) / dc.qty_per_box)) dm_qty
    FROM
        vartefact.forecast_calendar cal
        INNER JOIN vartefact.forecast_calendar wst ON wst.week_index = cal.week_index
        AND wst.weekday_short = 'Mon'
        INNER JOIN vartefact.v_forecast_inscope_dc_item_details dc ON dc.rotation = 'X'
        LEFT OUTER JOIN vartefact.forecast_xdock_orders ord ON ord.order_day = cal.date_key
        AND ord.item_code = dc.item_code
        AND ord.sub_code = dc.sub_code
        AND ord.dept_code = dc.dept_code
        AND ord.order_day >='{0}'
        AND ord.order_day <'{2}'
        LEFT OUTER JOIN vartefact.forecast_dm_orders dm ON ord.store_code = dm.store_code
        AND ord.dept_code = dm.dept_code
        AND ord.item_code = dm.item_code
        AND ord.sub_code = dm.sub_code
        AND ord.order_day = dm.first_order_date
        AND dm.first_order_date >='{0}'
        AND dm.first_order_date <'{2}'
        LEFT OUTER JOIN vartefact.service_level_safety2_vinc sl ON ord.item_code = sl.item_code
        AND ord.sub_code = sl.sub_code
        AND ord.dept_code = sl.dept_code
    WHERE
        wst.date_key >='{0}'
        and wst.date_key <='{1}'
    GROUP BY
        dc.dept_code,
        dc.item_code,
        dc.sub_code,
        wst.date_key 
        """.replace("\n", " ") \
        .format(
                w1_start_date.strftime("%Y%m%d"), 
                w9_start_date.strftime("%Y%m%d"),
                w10_start_date.strftime("%Y%m%d"))

xdock_orders = sqlc.sql(xdock_orders_sql)
# -

xdock_orders_df = xdock_orders.toPandas()

# # DC orders

# +
dc_orders_sql = """
    SELECT
        dc.dept_code,
        dc.item_code,
        dc.sub_code,
        wst.date_key week_start_day,
        sum(
            ceil(
                coalesce(ord.order_qty, 0) * (2 - coalesce(sl.service_level, 1)) / dc.qty_per_box
            )
        ) order_qty,
        sum(ceil(coalesce(dm.order_qty, 0) / dc.qty_per_box)) dm_qty
    FROM
        vartefact.forecast_calendar cal
        INNER JOIN vartefact.forecast_calendar wst ON wst.week_index = cal.week_index
        AND wst.weekday_short = 'Mon'
        INNER JOIN vartefact.v_forecast_inscope_dc_item_details dc ON dc.rotation != 'X'
        LEFT OUTER JOIN vartefact.forecast_dc_orders ord ON ord.order_day = cal.date_key
        AND ord.item_code = dc.item_code
        AND ord.sub_code = dc.sub_code
        AND ord.dept_code = dc.dept_code
        AND ord.order_day >='{0}'
        AND ord.order_day <'{2}'
        LEFT OUTER JOIN vartefact.forecast_dm_dc_orders dm ON ord.dept_code = dm.dept_code
        AND ord.item_code = dm.item_code
        AND ord.sub_code = dm.sub_code
        AND ord.order_day = dm.first_order_date
        AND dm.first_order_date >='{0}'
        AND dm.first_order_date <'{2}'
        LEFT OUTER JOIN vartefact.service_level_safety2_vinc sl ON ord.item_code = sl.item_code
        AND ord.sub_code = sl.sub_code
        AND ord.dept_code = sl.dept_code
    WHERE
        wst.date_key >='{0}'
        and wst.date_key <='{1}'
    GROUP BY
        dc.dept_code,
        dc.item_code,
        dc.sub_code,
        wst.date_key
        """.replace("\n", " ") \
        .format(
                w1_start_date.strftime("%Y%m%d"), 
                w9_start_date.strftime("%Y%m%d"),
                w10_start_date.strftime("%Y%m%d"))


dc_orders = sqlc.sql(dc_orders_sql)
# -

dc_orders_df = dc_orders.toPandas()

all_forecast = pd.concat([xdock_orders_df, dc_orders_df],ignore_index=True)  

# # Forecast File

# +
output_path = "/data/jupyter/ws_house/Carrefour_DM"

run_date_str = run_date.strftime("%Y%m%d")

w1_date_str = w1_start_date.strftime("%Y%m%d")
w2_date_str = w2_start_date.strftime("%Y%m%d")
w3_date_str = w3_start_date.strftime("%Y%m%d")
w4_date_str = w4_start_date.strftime("%Y%m%d")
w5_date_str = w5_start_date.strftime("%Y%m%d")
w6_date_str = w6_start_date.strftime("%Y%m%d")
w7_date_str = w7_start_date.strftime("%Y%m%d")
w8_date_str = w8_start_date.strftime("%Y%m%d")
w9_date_str = w9_start_date.strftime("%Y%m%d")


# +
def get_order_qty (df, row, week_start_day):
    df_line = all_forecast[(all_forecast["item_code"] == row.item_code) 
                        & (all_forecast["sub_code"] == row.sub_code)
                       & (all_forecast["dept_code"] == row.dept_code)
                       & (all_forecast["week_start_day"] == week_start_day)]
    if len(df_line) > 0:
        return str(df_line['order_qty'].iloc[0])
    else:
        return "0"

def get_dm_qty (df, row, week_start_day):
    df_line = all_forecast[(all_forecast["item_code"] == row.item_code) 
                        & (all_forecast["sub_code"] == row.sub_code)
                       & (all_forecast["dept_code"] == row.dept_code)
                       & (all_forecast["week_start_day"] == week_start_day)]

    if len(df_line) > 0:
        return str(df_line['dm_qty'].iloc[0])
    else:
        return "0"
    
    
def write_forecast_file(con_holding, supplier_name, forecast_file, items_df, all_forecast):
    wb = Workbook()
    ws = wb.active
    ws.append(
        ['Supplier_name','Barcode','Department_code','Item_code',
         'Sub_code','Item_desc_chn','Item_desc_eng',
         f'Week1_{w1_date_str}_Permanent_Box', f'Week1_{w1_date_str}_DM_Box',
         f'Week2_{w2_date_str}_Permanent_Box', f'Week2_{w2_date_str}_DM_Box',
         f'Week3_{w3_date_str}_Permanent_Box', f'Week3_{w3_date_str}_DM_Box',
         f'Week4_{w4_date_str}_Permanent_Box', f'Week4_{w4_date_str}_DM_Box',
         f'Week5_{w5_date_str}_Permanent_Box', f'Week5_{w5_date_str}_DM_Box',
         f'Week6_{w6_date_str}_Permanent_Box', f'Week6_{w6_date_str}_DM_Box',
         f'Week7_{w7_date_str}_Permanent_Box', f'Week7_{w7_date_str}_DM_Box',
         f'Week8_{w8_date_str}_Permanent_Box', f'Week8_{w8_date_str}_DM_Box',
         f'Week9_{w9_date_str}_Permanent_Box', f'Week9_{w9_date_str}_DM_Box '])

    for index, row in items_df[items_df["holding_code"] == con_holding].iterrows():
        ws.append([supplier_name, row.primary_barcode,
                   row.dept_code, row.item_code, row.sub_code, row.item_name_local, row.item_name_english,
                   get_order_qty(all_forecast, row, w1_date_str), get_dm_qty(all_forecast, row, w1_date_str),
                   get_order_qty(all_forecast, row, w2_date_str), get_dm_qty(all_forecast, row, w2_date_str),
                   get_order_qty(all_forecast, row, w3_date_str), get_dm_qty(all_forecast, row, w3_date_str),
                   get_order_qty(all_forecast, row, w4_date_str), get_dm_qty(all_forecast, row, w4_date_str),
                   get_order_qty(all_forecast, row, w5_date_str), get_dm_qty(all_forecast, row, w5_date_str),
                   get_order_qty(all_forecast, row, w6_date_str), get_dm_qty(all_forecast, row, w6_date_str),
                   get_order_qty(all_forecast, row, w7_date_str), get_dm_qty(all_forecast, row, w7_date_str),
                   get_order_qty(all_forecast, row, w8_date_str), get_dm_qty(all_forecast, row, w8_date_str),
                   get_order_qty(all_forecast, row, w9_date_str), get_dm_qty(all_forecast, row, w9_date_str)])

    wb.save(output_path + '/' + forecast_file)


# +
con_holding = "700"
supplier_name = "Unilever Services (Hefei) Co. Ltd."

forecast_file = f"Carrefour_Order_Forecast_DC_level_{con_holding}_{run_date_str}.xlsx"

write_forecast_file(con_holding, supplier_name, forecast_file, items_df, all_forecast)

# +
con_holding = "002"
supplier_name = "Shanghai Nestle products Service Co.,Ltd"

forecast_file = f"Carrefour_Order_Forecast_DC_level_{con_holding}_{run_date_str}.xlsx"

write_forecast_file(con_holding, supplier_name, forecast_file, items_df, all_forecast)


# +
con_holding = "693"
supplier_name = "Procter&Gamble (China) Sales Co.,Ltd."

forecast_file = f"Carrefour_Order_Forecast_DC_level_{con_holding}_{run_date_str}.xlsx"

write_forecast_file(con_holding, supplier_name, forecast_file, items_df, all_forecast)
# -

sc.stop()
