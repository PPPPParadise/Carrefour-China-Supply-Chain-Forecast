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

import math
import warnings
import pandas as pd

# +
warnings.filterwarnings('ignore')

store_item = pd.read_excel('store_item.xlsx', 'ordinary', header=0, dtype=str).fillna("")

store_item = store_item[store_item['CON_HOLDING'].isin(['002', '693', '700'])]

x_rotation_mapping = store_item[store_item["ROTATION"] == "X"]


# +
def get_order_weekday(order_day):
    if order_day == "MON.":
        return 1
    elif order_day == "TUE.":
        return 2
    elif order_day == "WED.":
        return 3
    elif order_day == "THU.":
        return 4
    elif order_day == "FRI.":
        return 5
    elif order_day == "SAT.":
        return 6
    elif order_day == "SUN.":
        return 7

def get_deliver_weekday(order_day, lead_time):
    delivery_day = order_day + lead_time
    
    if delivery_day > 7:
        return delivery_day % 7
    return delivery_day


def get_week_shift(order_day, lead_time):
    if (order_day + lead_time) > 7:
        return math.floor((order_day + lead_time) / 7)
    return 0


# +
new_mapping = x_rotation_mapping[["DEPT code", "ITEM_CODE", "Sub Code",  "Flow type",
                                  "ROTATION", "X rotation orderday", "X rotation LT"]].drop_duplicates()

new_mapping = new_mapping.set_index(["DEPT code", "ITEM_CODE", "Sub Code", "Flow type",
                                     "ROTATION", "X rotation LT"])

new_mapping = new_mapping.stack().str.split('/', expand=True) \
    .stack().apply(pd.Series).stack() \
    .unstack(level=8).reset_index(-1, drop=True).reset_index()


# +
new_mapping.columns = ["dept_code", 'item_code', 'sub_code', 'flow_type', 'rotation',
                       'lead_time', 'dummy1', 'order_day']

new_mapping = new_mapping[['dept_code','item_code', 'sub_code', 'flow_type', 'rotation', 'lead_time', 'order_day']]

new_mapping.lead_time = new_mapping.lead_time.str.split(' ', 1, expand=True)

new_mapping["order_weekday"] = new_mapping.apply(lambda r: get_order_weekday(r.order_day), axis=1)

new_mapping["delivery_weekday"] = new_mapping.apply(
    lambda r: get_deliver_weekday(r.order_weekday, int(r.lead_time)), axis=1)

new_mapping["week_shift"] = new_mapping.apply(lambda r: get_week_shift(r.order_weekday, int(r.lead_time)), axis=1)


# +
from load_spark import load_spark
from pyspark.sql import HiveContext

sc = load_spark("prepare_item_tables")

sqlc = HiveContext(sc)
# -

mapping_df = sqlc.createDataFrame(new_mapping)

mapping_df.write.mode("overwrite").saveAsTable("vartefact.xdock_order_delivery_mapping")

item_details = store_item[['DEPT code', 'CON_HOLDING', 'HOLDING_NAME', 'HOLDING_CHN_NAME',
       'ITEM_CODE', 'Sub Code', 'LOCAL_NAME', 'PCB', 'Flow type',
       'ROTATION', 'DC Supplier', 'DS Supplier', 'Item status',
       'Store need to stop when W stock=o', 'Cover Region']]

# +
item_details.columns = ['dept_code', 'con_holding', 'holding_name', 'holiding_chn_name',
       'item_code', 'sub_code', 'local_name', 'pcb', 'flow_type',
       'rotation', 'dc_supplier_code', 'ds_supplier_code', 'item_status',
       'store_stop_when_stock_is_o', 'cover_region']

item_details = item_details.drop_duplicates()

item_details_df = sqlc.createDataFrame(item_details)
# -

item_details_df.write.mode("overwrite").saveAsTable("vartefact.forecast_item_details")

sc.stop()


