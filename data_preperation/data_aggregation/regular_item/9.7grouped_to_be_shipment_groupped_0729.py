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

# # Setup environment

# +
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SQLContext 

from pyspark.sql import SparkSession
import os, sys, gc, datetime, time

USE_SPARK = 1
if USE_SPARK:
    print('Trying to get spark connection...')
    warehouse_location = os.path.abspath('spark-warehouse')

    spark = (SparkSession \
        .builder \
        .appName("Qiang (Charles)") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.num.executors", '15') \
        .config("spark.executor.memory", '20G') \
        .config("spark.executor.cores", '25') \
        .enableHiveSupport() \
        .getOrCreate()
    ) 
    print('Spark connection created!')

sqlc = SQLContext(spark)

# /* ===================== getopt added ==========================
# example: python3.6 ./sqls/9.7grouped_to_be_shipment_groupped_0729.py -d {config['database']}
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database_name", help="database name")
args = parser.parse_args()

print(args.database_name)

config = {}
config['database'] = args.database_name

# config = {}
# config['database'] = 'temp'

def timed_pull(query):
    '''Convenient function to pull data by SQL with timer function.'''
    foo = spark.sql(query).toPandas()
    print(f'Dataset size = {foo.shape}, {foo.memory_usage().sum() / 2**20:.2f} MB.')
    return foo


sqlStr = \
f"""
SELECT 
    cast(taaf.trxn_date as timestamp) trxn_date,
    taaf.group_id,
    taaf.sales_qty,
    vfts.*
 FROM {config['database']}.shipment_scope_map_corrected vfts
 JOIN {config['database']}.grouped_to_be_shipment taaf 
     ON taaf.item_id = vfts.item_id
    AND taaf.sub_id = vfts.sub_id
    AND taaf.store_code = vfts.store_code
    AND abs(datediff(vfts.delivery_date, taaf.trxn_date)) <= 35
    AND taaf.art_flag = 1
"""
df = timed_pull(sqlStr)


items = df.groupby(['item_id', 'sub_id', 'store_code'])['trxn_date'].count().reset_index().drop(columns='trxn_date')


markedTrxn = []
counter = 1
# trxnFile = open("/data/jupyter/ws_house/trxn.txt", "a")

for i, item in items.iterrows():
#     if (i + 1) % 100 == 0:
#         print("loop ", counter, " of ", len(items))
    shipments = df[(df.item_id == item.item_id) &
                   (df.sub_id == item.sub_id) &
                   (df.store_code == item.store_code)
                  ].drop_duplicates(subset=['delivery_date'])[
        ['delivery_date', 'delivery_qty_sum']
    ].reset_index(drop=True)
    trxns = df[(df.item_id == item.item_id) &
               (df.sub_id == item.sub_id) &
               (df.store_code == item.store_code)
              ].drop_duplicates(subset=['trxn_date'])[
        ['trxn_date', 'item_id', 'sub_id', 'store_code', 'group_id', 'sales_qty']
    ].reset_index(drop=True)
    #display(shipments, trxns)
    
    shipments = dict(shipments.iterrows())
    trxns = dict(trxns.iterrows())
    for k, v in shipments.items():
        shipments[k] = v.to_dict()
    for k, v in trxns.items():
        trxns[k] = v.to_dict()
    
    i = 0
    j = 0
    while i < len(shipments):
        trxnTmp = {}
        shipmentQty = shipments[i]["delivery_qty_sum"]
        while shipmentQty > 0 and j < len(trxns):
            
            if abs(shipments[i]["delivery_date"] - trxns[j]["trxn_date"]).days >= 35:
                break
                
            if shipmentQty >= trxns[j]["sales_qty"]:
                shipmentQty = shipmentQty - trxns[j]["sales_qty"]
                trxnTmp["delivery_date"]= shipments[i]["delivery_date"]
                trxnTmp["delivery_qty_sum"]= shipments[i]["delivery_qty_sum"]
                trxnTmp["counter"]= counter
                
                #trxnTmp = pd.DataFrame(trxns[j]).transpose()
                trxnTmp = pd.DataFrame(trxns[j].copy().update(trxnTmp)).transpose()
                markedTrxn.append(trxns[j])
                trxnTmp.to_csv('trxn_with_shipments.csv',mode='a',index=False, header=False)
                
            elif shipmentQty > 0:
                shipmentQty = 0

                trxnTmp["delivery_date"]= shipments[i]["delivery_date"]
                trxnTmp["delivery_qty_sum"]= shipments[i]["delivery_qty_sum"]
                trxnTmp["counter"]= counter
                
                #trxnTmp = pd.DataFrame(trxns[j]).transpose()
                trxnTmp = pd.DataFrame(trxns[j].copy().update(trxnTmp)).transpose()
                markedTrxn.append(trxns[j])
                trxnTmp.to_csv('trxn_with_shipments.csv',mode='a',index=False, header=False)
                
            j = j+1
        i = i+1
    counter = counter +1

marked = [list(i.values()) for i in markedTrxn]

cols = list(markedTrxn[0].keys())
markedTrxn_dict = {}
for k in cols:
    markedTrxn_dict[k] = []
for markedTrxn_e in markedTrxn:
    for k in cols:
        markedTrxn_dict[k].append(markedTrxn_e[k])

markedTrxnDf = pd.DataFrame(markedTrxn_dict)

markedTrxnDf = sqlc.createDataFrame(markedTrxnDf)

markedTrxnDf.write.mode("overwrite").saveAsTable(f"{config['database']}.grouped_to_be_shipment_groupped")

spark.stop()

