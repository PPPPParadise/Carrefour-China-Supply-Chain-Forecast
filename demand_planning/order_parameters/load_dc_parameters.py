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
def get_qty_per_unit(row):

    if row['order_uint'] == 'box':
        return int(row['qty_per_pack']) * int(row['pack_per_box'])

    if row['order_uint'] == 'layer':
        return int(row['qty_per_pack']) * int(row['pack_per_box']) * int(row['box_per_layer_ti'])

    if row['order_uint'] == 'pallet':
        return int(row['qty_per_pack']) * int(row['pack_per_box']) \
               * int(row['box_per_layer_ti']) * int(row['layer_per_pallet_hi'])
    
    return int(row['qty_per_pack']) * int(row['pack_per_box'])    

def get_risk_item_unilever(row):
    if row['risk_item_unilever'] == 'Y':
        return "Y"
    return "N"


def get_conholding(ds_supplier_code):
    if ds_supplier_code == "06WC" or ds_supplier_code == "1GTB":
        return "002"
    if ds_supplier_code == "ZB09":
        return "693"
    if ds_supplier_code == "0031" or ds_supplier_code == "N389":
        return "700"
    return ""
# -

# # Info

# Use this file to load the list of all in scope DC items and their information. Such as order unit.


# +
warnings.filterwarnings('ignore')

dc_items = pd.read_excel('East 3 Supps DC Item list 20190920.xlsx', 'Item Detail', header=0, dtype=str).fillna('')
# -

dc_items.rename(columns={'Item code':'Full item code'}, inplace=True)

dc_items["Dept code"] = dc_items["Full item code"].str.slice(0, 2)
dc_items["Item code"] = dc_items["Full item code"].str.slice(2, 8)
dc_items["Sub code"] = dc_items["Full item code"].str.slice(8)

dc_items.columns = ['dc', 'dc_site', 'full_item_code', 'item_name_english',
'item_name_local', 'current_warehouse', 'primary_ds_supplier',
'primary_ds_supplier_name', 'qty_per_box', 'primary_barcode',
'rotation', 'box_per_layer_ti', 'layer_per_pallet_hi',
'stop_start_date', 'stop_reason', 'qty_per_pack', 'pack_per_box',
'holding_supplier_code', 'holding_code', 'risk_item_unilever',
'order_uint', 'dc_status', 'item_type',
'dept_code', 'item_code', 'sub_code']

dc_items["holding_code"] = dc_items.apply(
    lambda r: get_conholding(r.primary_ds_supplier), axis=1)

dc_items["rotation"] = dc_items["rotation"].str.strip()
dc_items["rotation"] = dc_items["rotation"].str.upper()

dc_items['qty_per_unit'] = dc_items.apply(get_qty_per_unit, axis = 1)

dc_items['risk_item_unilever'] = dc_items.apply(get_risk_item_unilever, axis = 1)

# # Write to datalake

# +
from load_spark import load_spark
from pyspark.sql import HiveContext

sc = load_spark("Load DC parameter")

sqlc = HiveContext(sc)

# +
dc_items_df = sqlc.createDataFrame(dc_items)

dc_items_df = dc_items_df.withColumn("qty_per_unit", dc_items_df["qty_per_unit"].cast("Int"))

dc_items_df = dc_items_df.withColumn("qty_per_box", dc_items_df["qty_per_box"].cast("Int"))

dc_items_df = dc_items_df.withColumn("box_per_layer_ti", dc_items_df["box_per_layer_ti"].cast("Int"))

dc_items_df = dc_items_df.withColumn("layer_per_pallet_hi", dc_items_df["layer_per_pallet_hi"].cast("Int"))

dc_items_df = dc_items_df.withColumn("qty_per_pack", dc_items_df["qty_per_pack"].cast("Int"))

dc_items_df = dc_items_df.withColumn("pack_per_box", dc_items_df["pack_per_box"].cast("Int"))

dc_items_df.write.mode("overwrite").saveAsTable("vartefact.forecast_dc_item_details")
# -

sc.stop()
