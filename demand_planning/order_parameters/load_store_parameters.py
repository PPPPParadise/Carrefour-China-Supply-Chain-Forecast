# -*- coding: utf-8 -*-
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
def get_deliver_weekday(order_day, lead_time):
    if (order_day + lead_time) > 7:
        return (order_day + lead_time) % 7
    return (order_day + lead_time)


def get_week_shift(order_day, lead_time):
    if (order_day + lead_time) > 7:
        return math.floor((order_day + lead_time) / 7)
    return 0


def get_conholding(ds_supplier_code):
    if ds_supplier_code == "06WC" or ds_supplier_code == "1GTB":
        return "002"
    if ds_supplier_code == "ZB09":
        return "693"
    if ds_supplier_code == "0031" or ds_supplier_code == "N389":
        return "700"
    return ""


def get_qty_per_unit(row):

    if row['order_by'] == 'S':
        return int(row['qty_per_pack'])
    
    return int(row['qty_per_pack']) * int(row['pack_per_box'])      

def get_store_to_dc_day(row):
    return int(1) 


# -

# # Read Excel

# +
warnings.filterwarnings('ignore')

excel_input = pd.read_excel('East Parameter 20190927.xlsx', 'Detail', header=0, dtype=str).fillna("")

store_delivery = pd.read_excel('store_delivery_time.xlsx', 'Sheet1', header=0, dtype=str).fillna("")
# -

# # Get store item list

store_items = excel_input[["Store", "Dept", "Item code", "sub code", "CN Name", "Store Status",
                   "Main Supplier", "DS Supplier", "Order day", "LT" , "Order by",
                   "Qty/Pack", "Pack/Box", "DC_status", "Rotation", "ItemType", "OrigItemType", 
                    "Risk Item (Unilever)", "Item inputdate"]].drop_duplicates().reset_index(drop=True)

store_items.columns = ["store_code", "dept_code", "item_code", "sub_code", "cn_name", "store_status",
                   "dc_supplier_code", "ds_supplier_code", "order_day", "lead_time", "order_by",
                   "qty_per_pack", "pack_per_box", "dc_status", "rotation", "item_type", "orig_item_type",
                    "risk_item_unilever", "item_inputdate"]

store_items['qty_per_unit'] = store_items.apply(get_qty_per_unit, axis = 1)

store_items["con_holding"] = store_items.apply(
    lambda r: get_conholding(r.ds_supplier_code), axis=1)

# # Get order delivery mapping for on stock

order_frequency = store_items[["store_code", "dept_code", "order_day", "lead_time", "rotation"]]

order_frequency = order_frequency[order_frequency["rotation"] != "X"].drop_duplicates().reset_index(drop=True)

# +
order_frequency_mapping = []

for index, freq in order_frequency.iterrows():
    if freq.order_day.find('1') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "1" , "Mon"])
    if freq.order_day.find('2') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "2" , "Tue"])
    if freq.order_day.find('3') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "3" , "Wed"])
    if freq.order_day.find('4') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "4" , "Thu"])
    if freq.order_day.find('5') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "5", "Fri"])
    if freq.order_day.find('6') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "6", "Sat"])
    if freq.order_day.find('7') >= 0:
        order_frequency_mapping.append([freq.store_code, freq.dept_code, 
                                        freq.rotation, freq.lead_time, freq.order_day, "7", "Sun"])
# -

order_days_mapping = pd.DataFrame(order_frequency_mapping)

order_days_mapping.columns = ["store_code", "dept_code", "rotation", "lead_time", "order_days", 
                              "order_iso_weekday", "order_weekday_short"]

# +
order_days_mapping["delivery_iso_weekday"] = order_days_mapping.apply(
    lambda r: get_deliver_weekday(int(r.order_iso_weekday), int(r.lead_time)), axis=1)

order_days_mapping["week_shift"] = order_days_mapping.apply(lambda r: get_week_shift(int(r.order_iso_weekday), int(r.lead_time)), axis=1)
# -

# # Get order delivery mapping for cross docking

order_x_frequency = store_items[["store_code", "dept_code", "item_code", "sub_code", 
                                 "order_day", "lead_time", "rotation", "risk_item_unilever"]]

order_x_frequency = order_x_frequency[order_x_frequency["rotation"] == "X"].drop_duplicates().reset_index(drop=True)

order_x_frequency["dc_to_store_time"] = order_x_frequency.apply(get_store_to_dc_day, axis = 1)

# +
order_x_frequency_mapping = []

for index, freq in order_x_frequency.iterrows():
    if freq.order_day.find('1') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "1" , "Mon"])
    if freq.order_day.find('2') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "2" , "Tue"])
    if freq.order_day.find('3') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "3" , "Wed"])
    if freq.order_day.find('4') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "4" , "Thu"])
    if freq.order_day.find('5') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "5", "Fri"])
    if freq.order_day.find('6') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "6", "Sat"])
    if freq.order_day.find('7') >= 0:
        order_x_frequency_mapping.append([freq.store_code, freq.dept_code, freq.item_code, freq.sub_code,
                                        freq.lead_time, freq.dc_to_store_time, freq.order_day, "7", "Sun"])
# -
xdock_order_mapping = pd.DataFrame(order_x_frequency_mapping)

xdock_order_mapping.columns = ["store_code", "dept_code", "item_code", "sub_code", "lead_time", 
                               "dc_to_store_time", "order_days", "order_iso_weekday", "order_weekday_short"]

store_items.head()

order_days_mapping.head()

xdock_order_mapping.head()

# +
from load_spark import load_spark
from pyspark.sql import HiveContext

sc = load_spark("Load store parameter")

sqlc = HiveContext(sc)

# +
store_items_df = sqlc.createDataFrame(store_items)

store_items_df = store_items_df.withColumn("qty_per_unit", store_items_df["qty_per_unit"].cast("Int"))

store_items_df = store_items_df.withColumn("pack_per_box", store_items_df["pack_per_box"].cast("Int"))

store_items_df = store_items_df.withColumn("qty_per_pack", store_items_df["qty_per_pack"].cast("Int"))

store_items_df = store_items_df.withColumn("lead_time", store_items_df["lead_time"].cast("Int"))

store_items_df.write.mode("overwrite").saveAsTable("vartefact.forecast_store_item_details")

# +
order_days_mapping_df = sqlc.createDataFrame(order_days_mapping)

order_days_mapping_df = order_days_mapping_df.withColumn("delivery_iso_weekday", order_days_mapping_df["delivery_iso_weekday"].cast("String"))

order_days_mapping_df = order_days_mapping_df.withColumn("week_shift", order_days_mapping_df["week_shift"].cast("Int"))

order_days_mapping_df.write.mode("overwrite").saveAsTable("vartefact.forecast_onstock_order_delivery_mapping")

# +
xdock_order_mapping_df = sqlc.createDataFrame(xdock_order_mapping)

xdock_order_mapping_df = xdock_order_mapping_df.withColumn("dc_to_store_time", xdock_order_mapping_df["dc_to_store_time"].cast("Int"))

xdock_order_mapping_df.write.mode("overwrite").saveAsTable("vartefact.forecast_xdock_order_mapping")
# -

store_delivery_df = sqlc.createDataFrame(store_delivery[['store_code', 'delivery_time']])

store_delivery_df.write.mode("overwrite").saveAsTable("vartefact.forecast_stores_delv_time")

sc.stop()


